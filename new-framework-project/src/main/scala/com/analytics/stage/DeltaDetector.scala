package com.analytics.stage

import org.apache.spark.sql.DataFrame

/**
 * DeltaDetector — identifies rows that are new or changed between two snapshots.
 *
 * The comparison is purely hash-key based: a stage row whose __hash_key is absent
 * from the committed snapshot represents either:
 *   (a) a brand-new record (no matching PK in committed), or
 *   (b) an updated record (PK exists in committed but content hash has changed).
 *
 * Both cases are surfaced as "delta" and handled identically downstream — the
 * StageManager merges them into committed using the model's natural primary keys,
 * and the SemanticLayerPipeline upserts them into Snowflake.
 *
 * Unchanged rows (hash present in both stage and committed) are deliberately
 * excluded to keep Snowflake writes minimal.
 */
object DeltaDetector {

  /**
   * Returns the subset of stageDF whose __hash_key is NOT present in committedDF.
   * A left-anti join on the hash column is used so the comparison stays in the
   * Spark execution engine with no driver-side collect.
   */
  def detectDelta(stageDF: DataFrame, committedDF: DataFrame): DataFrame = {
    val committedHashes = committedDF
      .select(HashKeyBuilder.HASH_KEY_COL)
      .distinct()

    stageDF.join(committedHashes, Seq(HashKeyBuilder.HASH_KEY_COL), "left_anti")
  }

  /**
   * Convenience overload that returns the count of delta records without
   * materialising the full DataFrame — useful for quick guard checks.
   */
  def deltaCount(stageDF: DataFrame, committedDF: DataFrame): Long =
    detectDelta(stageDF, committedDF).count()

  /**
   * Returns delta hash keys as a driver-side Set.
   * Only call this for small cardinality datasets; prefer detectDelta for large ones.
   */
  def getDeltaHashKeys(stageDF: DataFrame, committedDF: DataFrame): Set[String] =
    HashKeyBuilder.extractHashKeySet(detectDelta(stageDF, committedDF))
}
