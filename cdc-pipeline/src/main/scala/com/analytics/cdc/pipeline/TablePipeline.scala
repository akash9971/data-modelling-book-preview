package com.analytics.cdc.pipeline

import com.analytics.cdc.config.TableConfig
import com.analytics.cdc.engine.CdcStrategy
import com.analytics.cdc.factory.Factory
import com.analytics.cdc.model.{CdcColumns, RecordState}
import com.analytics.cdc.util.Util
import com.analytics.cdc.writer.EmoWriter
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, current_timestamp, lit}
import org.slf4j.LoggerFactory

class TablePipeline(strategy: CdcStrategy) {
  private val logger        = LoggerFactory.getLogger(getClass)
  private val parquetFormat = Factory.format("parquet")
  private val deltaFormat   = Factory.format("delta")

  def run(spark: SparkSession, table: TableConfig, snapDate: String, force: Boolean = false): Unit = {
    if (!force && alreadyProcessed(spark, table, snapDate)) {
      logger.info(s"[${table.modelName}] snap_date=$snapDate already processed — skipping (use force=true to reprocess)")
      return
    }

    val hashed = Util.withJobGroup(spark, s"${table.modelName}-read", s"Read+filter+derive @ $snapDate") {
      val raw      = parquetFormat.read(spark, s"${table.lmoPath}/$snapDate")
      val filtered = table.filter.map(raw.filter).getOrElse(raw)
      val renamed  = Util.applyMappings(filtered, table.mappings)
      val derived  = Util.applyDerivations(renamed, table.derivations)
      Util.addContentHash(derived)
    }

    val activeEmo =
      if (deltaFormat.exists(spark, table.emoPath))
        Some(deltaFormat.read(spark, table.emoPath).where(col(CdcColumns.OpType) === RecordState.Active.label))
      else None

    val diff = Util.withJobGroup(spark, s"${table.modelName}-diff", "Compute CDC diff vs active emo") {
      val result = strategy.diff(hashed, activeEmo, table.primaryKeys)
      // Cut lineage eagerly: applyLifecycleUpdate below mutates table.emoPath on disk, and these
      // DataFrames are lazy plans that (transitively, via activeEmo) read from that same path.
      // persist() alone is not enough — Delta's write/update invalidates Spark's cache for that
      // path, so a persisted-but-invalidated DataFrame silently recomputes from its original
      // lineage, re-reading the now-mutated table (an UPDATE would be misread as an INSERT at
      // version 1). localCheckpoint() truncates lineage entirely so no such recomputation can
      // happen, regardless of cache invalidation.
      val newVersionRows = result.newVersionRows.localCheckpoint()
      val expiredKeys    = result.expiredKeys.localCheckpoint()
      val deletedKeys    = result.deletedKeys.localCheckpoint()
      result.copy(newVersionRows = newVersionRows, expiredKeys = expiredKeys, deletedKeys = deletedKeys)
    }

    Util.withJobGroup(spark, s"${table.modelName}-write", "Apply lifecycle update + append new versions") {
      EmoWriter.applyLifecycleUpdate(spark, table.emoPath, diff.expiredKeys, diff.deletedKeys, table.primaryKeys)

      val toAppend = diff.newVersionRows
        .withColumn(CdcColumns.SnapDate, lit(snapDate))
        .withColumn(CdcColumns.EffectiveTs, current_timestamp())

      EmoWriter.appendNew(spark, deltaFormat, table.emoPath, toAppend)
    }

    logger.info(s"[${table.modelName}] snap_date=$snapDate processed successfully")
  }

  private def alreadyProcessed(spark: SparkSession, table: TableConfig, snapDate: String): Boolean =
    deltaFormat.exists(spark, table.emoPath) &&
      deltaFormat.read(spark, table.emoPath).where(col(CdcColumns.SnapDate) === snapDate).limit(1).count() > 0
}
