package com.analytics.cdc.engine

import com.analytics.cdc.model.{CdcColumns, CdcOp, RecordState}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class CdcDiffResult(
  newVersionRows: DataFrame,
  expiredKeys:    DataFrame,
  deletedKeys:    DataFrame
)

trait CdcStrategy {
  def diff(today: DataFrame, activeEmo: Option[DataFrame], primaryKeys: List[String]): CdcDiffResult
}

object HashBasedCdcStrategy extends CdcStrategy {

  private val ActiveHashAlias    = "__active_hash"
  private val ActiveVersionAlias = "__active_version"

  override def diff(today: DataFrame, activeEmo: Option[DataFrame], primaryKeys: List[String]): CdcDiffResult =
    activeEmo match {
      case None          => bootstrap(today, primaryKeys)
      case Some(active)  => diffAgainstActive(today, active, primaryKeys)
    }

  private def bootstrap(today: DataFrame, primaryKeys: List[String]): CdcDiffResult = {
    val inserts = today
      .withColumn(CdcColumns.PrevHashKey, lit(null: String))
      .withColumn(CdcColumns.Version, lit(1))
      .withColumn(CdcColumns.OpName, lit(CdcOp.Insert.label))
      .withColumn(CdcColumns.OpType, lit(RecordState.Active.label))

    CdcDiffResult(
      newVersionRows = inserts,
      expiredKeys    = emptyKeyFrame(today, primaryKeys),
      deletedKeys    = emptyKeyFrame(today, primaryKeys)
    )
  }

  private def diffAgainstActive(today: DataFrame, active: DataFrame, primaryKeys: List[String]): CdcDiffResult = {
    val pkCols = primaryKeys.map(col)

    val activeSlim = active.select(
      (pkCols :+ col(CdcColumns.HashKey).as(ActiveHashAlias) :+ col(CdcColumns.Version).as(ActiveVersionAlias)): _*
    )

    val insertRows = today.join(activeSlim, primaryKeys, "left_anti")
      .withColumn(CdcColumns.PrevHashKey, lit(null: String))
      .withColumn(CdcColumns.Version, lit(1))
      .withColumn(CdcColumns.OpName, lit(CdcOp.Insert.label))
      .withColumn(CdcColumns.OpType, lit(RecordState.Active.label))

    val joined  = today.join(activeSlim, primaryKeys, "inner")
    val changed = joined.where(col(CdcColumns.HashKey) =!= col(ActiveHashAlias))

    val updateRows = changed
      .withColumn(CdcColumns.PrevHashKey, col(ActiveHashAlias))
      .withColumn(CdcColumns.Version, col(ActiveVersionAlias) + 1)
      .withColumn(CdcColumns.OpName, lit(CdcOp.Update.label))
      .withColumn(CdcColumns.OpType, lit(RecordState.Active.label))
      .drop(ActiveHashAlias, ActiveVersionAlias)

    val expiredKeys = changed.select(pkCols: _*)

    val deletedKeys = activeSlim.join(today, primaryKeys, "left_anti")
      .select(pkCols: _*)

    CdcDiffResult(
      newVersionRows = insertRows.unionByName(updateRows),
      expiredKeys    = expiredKeys,
      deletedKeys    = deletedKeys
    )
  }

  private def emptyKeyFrame(today: DataFrame, primaryKeys: List[String]): DataFrame =
    today.filter(lit(false)).select(primaryKeys.map(col): _*)
}
