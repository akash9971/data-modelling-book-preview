package com.analytics.stage

import com.analytics.format.{DataFormat, DeltaFormat}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * StageManager — owns the lifecycle of stage and committed folders for one model.
 *
 * Stage folder
 * ────────────
 * Written on every run (full or incremental) with model output + hash columns.
 * Overwritten unconditionally each time — it represents "what this run produced".
 *
 * Committed folder
 * ────────────────
 * The durable, accepted snapshot of all rows that have been successfully shipped
 * to the Snowflake semantic layer. Populated by promoteToCommitted after a
 * successful full-load Snowflake write.  On incremental runs it is updated via
 * mergeDeltaIntoCommitted — only the delta rows (new/changed) are added/replaced.
 *
 * Both folders store __hash_key and __hash_date alongside all business columns.
 */
class StageManager(format: DataFormat) {

  // ---------------------------------------------------------------------------
  // WRITE / PROMOTE
  // ---------------------------------------------------------------------------

  /** Appends hash columns to df and writes the result to stagePath (overwrite).
   *  Returns the stage DataFrame (including hash columns) for immediate use. */
  def writeToStage(
    spark:       SparkSession,
    df:          DataFrame,
    stagePath:   String,
    hashColumns: Option[List[String]]
  ): DataFrame = {
    val dfWithHash = HashKeyBuilder.addHashKey(df, hashColumns)
    format.write(spark, dfWithHash, stagePath, "overwrite")
    println(s"    Stage written: $stagePath (${dfWithHash.count()} records)")
    dfWithHash
  }

  /** Copies the current stage snapshot into committedPath.
   *  Called by SemanticLayerPipeline after a successful full-load Snowflake write. */
  def promoteToCommitted(
    spark:         SparkSession,
    stagePath:     String,
    committedPath: String
  ): Unit = {
    val stageDF = format.read(spark, stagePath)
    format.write(spark, stageDF, committedPath, "overwrite")
    println(s"    Stage → committed: $committedPath (${stageDF.count()} records)")
  }

  // ---------------------------------------------------------------------------
  // DELTA MERGE INTO COMMITTED
  // ---------------------------------------------------------------------------

  /** Merges deltaDF (rows from stage that are new or changed) into committedPath.
   *
   *  Strategy by primary key availability:
   *    - primaryKeys non-empty + Delta format → native DeltaTable.merge (atomic upsert)
   *    - primaryKeys non-empty + other format → left-anti remove stale rows + union
   *    - primaryKeys empty (flat/reference)   → union and dedup on __hash_key
   *
   *  deltaDF must include __hash_key and __hash_date columns in addition to all
   *  business columns so the committed snapshot stays hash-comparable. */
  def mergeDeltaIntoCommitted(
    spark:         SparkSession,
    deltaDF:       DataFrame,
    committedPath: String,
    primaryKeys:   List[String]
  ): Unit = {
    if (!format.exists(spark, committedPath)) {
      // No committed state yet — delta becomes the initial committed snapshot
      format.write(spark, deltaDF, committedPath, "overwrite")
      println(s"    Committed initialised from delta: $committedPath")
      return
    }

    if (primaryKeys.isEmpty) {
      // Flat / reference tables: no natural PK — append and dedup on hash key
      val existing = format.read(spark, committedPath)
      val merged   = existing
        .union(deltaDF)
        .dropDuplicates(HashKeyBuilder.HASH_KEY_COL)
      format.write(spark, merged, committedPath, "overwrite")
      println(s"    Committed updated (hash dedup, no PK): $committedPath")
      return
    }

    format match {
      case _: DeltaFormat =>
        mergeDeltaViaDeltaMerge(spark, deltaDF, committedPath, primaryKeys)

      case _ =>
        mergeDeltaGeneric(spark, deltaDF, committedPath, primaryKeys)
    }
  }

  private def mergeDeltaViaDeltaMerge(
    spark:         SparkSession,
    deltaDF:       DataFrame,
    committedPath: String,
    primaryKeys:   List[String]
  ): Unit = {
    val target       = DeltaTable.forPath(spark, committedPath)
    val joinCond     = primaryKeys.map(pk => s"target.$pk = source.$pk").mkString(" AND ")
    val updateCols   = deltaDF.columns
      .filterNot(c => primaryKeys.contains(c))
      .map(c => c -> s"source.$c")
      .toMap

    target.as("target")
      .merge(deltaDF.as("source"), joinCond)
      .whenMatched().updateExpr(updateCols)
      .whenNotMatched().insertAll()
      .execute()

    println(s"    Committed updated via Delta MERGE: $committedPath")
  }

  private def mergeDeltaGeneric(
    spark:         SparkSession,
    deltaDF:       DataFrame,
    committedPath: String,
    primaryKeys:   List[String]
  ): Unit = {
    val existing = format.read(spark, committedPath)

    // Remove rows in committed that are superseded by delta (matched on PKs)
    val unchanged = existing.join(
      deltaDF.select(primaryKeys.map(col): _*).distinct(),
      primaryKeys,
      "left_anti"
    )

    val merged = unchanged.unionByName(deltaDF)
    format.write(spark, merged, committedPath, "overwrite")
    println(s"    Committed updated via left-anti + union: $committedPath")
  }

  // ---------------------------------------------------------------------------
  // READ HELPERS
  // ---------------------------------------------------------------------------

  def readStage(spark: SparkSession, stagePath: String): DataFrame =
    format.read(spark, stagePath)

  def readCommitted(spark: SparkSession, committedPath: String): DataFrame =
    format.read(spark, committedPath)

  def stageExists(spark: SparkSession, stagePath: String): Boolean =
    format.exists(spark, stagePath)

  def committedExists(spark: SparkSession, committedPath: String): Boolean =
    format.exists(spark, committedPath)
}
