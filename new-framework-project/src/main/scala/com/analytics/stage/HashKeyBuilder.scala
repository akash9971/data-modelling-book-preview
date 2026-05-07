package com.analytics.stage

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.time.LocalDate

/**
 * HashKeyBuilder — adds a deterministic row-level fingerprint to a DataFrame.
 *
 * The hash encodes both content and processing date so that the same record
 * arriving on two different days produces two different keys; this is the
 * primary mechanism used by DeltaDetector to identify new and changed rows.
 *
 * Hash formula:  SHA-256( col1 | col2 | ... | colN | YYYY-MM-DD )
 *
 * Column selection:
 *   - If hashColumns is Some(list) → use those columns (skipping any that are
 *     absent from the DataFrame, with a warning).
 *   - If hashColumns is None       → use every data column (excluding the two
 *     system columns added by this object itself).
 */
object HashKeyBuilder {

  val HASH_KEY_COL  = "__hash_key"
  val HASH_DATE_COL = "__hash_date"

  /** Appends __hash_key and __hash_date to df. */
  def addHashKey(df: DataFrame, hashColumns: Option[List[String]] = None): DataFrame = {
    val today    = LocalDate.now().toString
    val existing = df.columns.toSet

    val cols: Seq[String] = hashColumns match {
      case Some(specified) =>
        val missing = specified.filterNot(existing.contains)
        if (missing.nonEmpty)
          println(s"    [HashKeyBuilder] Warning: columns not found and skipped: ${missing.mkString(", ")}")
        specified.filter(existing.contains)

      case None =>
        df.columns.filterNot(c => c == HASH_KEY_COL || c == HASH_DATE_COL).toSeq
    }

    require(cols.nonEmpty, "HashKeyBuilder: no columns available to build hash key")

    val contentExpr  = concat_ws("|", cols.map(c => coalesce(col(c).cast("string"), lit("NULL"))): _*)
    val hashExpr     = sha2(concat(contentExpr, lit(s"|$today")), 256)

    df.withColumn(HASH_KEY_COL, hashExpr)
      .withColumn(HASH_DATE_COL, lit(today))
  }

  /** Collects the full set of hash key values from df as a Scala Set.
   *  Used for set-arithmetic delta detection when DataFrames are small enough
   *  to fit in driver memory. For large tables prefer DeltaDetector.detectDelta. */
  def extractHashKeySet(df: DataFrame): Set[String] =
    df.select(HASH_KEY_COL)
      .rdd
      .map(_.getString(0))
      .collect()
      .toSet
}
