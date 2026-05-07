package com.analytics.format

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import io.delta.tables._

/**
 * DataFormat — defines how to read, write, and check existence for a storage format.
 *
 * readIncremental: timestamp-based heuristic filter (last 24 h). When a primaryKey
 * is provided it is forwarded for logging only; actual PK-based watermarking is
 * handled by StageManager using the committed snapshot.
 *
 * Implementations are created by FormatFactory.
 */
trait DataFormat {
  def read(spark: SparkSession, path: String): DataFrame
  def write(spark: SparkSession, df: DataFrame, path: String, mode: String): Unit
  def exists(spark: SparkSession, path: String): Boolean

  /** Read only records newer than 24 hours, keyed on the first timestamp-like column found.
   *  primaryKey is accepted for logging; if no timestamp column is found the full dataset
   *  is returned so the pipeline never silently drops data. */
  def readIncremental(
    spark:      SparkSession,
    path:       String,
    primaryKey: Option[String] = None
  ): DataFrame = {
    val df = read(spark, path)
    val oneDayAgo = System.currentTimeMillis() - (24L * 60 * 60 * 1000)

    val tsCols = df.columns.filter(c =>
      c.toLowerCase.contains("date")      ||
      c.toLowerCase.contains("time")      ||
      c.toLowerCase.contains("created")   ||
      c.toLowerCase.contains("updated")   ||
      c.toLowerCase.contains("timestamp")
    )

    if (tsCols.nonEmpty) {
      val tsCol = tsCols.head
      val pkInfo = primaryKey.map(pk => s", PK=$pk").getOrElse("")
      println(s"    Incremental filter on column: $tsCol (last 24 h)$pkInfo")
      df.filter(col(tsCol) >= from_unixtime(lit(oneDayAgo / 1000)))
    } else {
      println(s"    Warning: no timestamp column found — falling back to full load")
      df
    }
  }
}

// =============================================================================
// DELTA LAKE
// =============================================================================

class DeltaFormat extends DataFormat {

  override def read(spark: SparkSession, path: String): DataFrame =
    spark.read.format("delta").load(path)

  override def write(spark: SparkSession, df: DataFrame, path: String, mode: String): Unit =
    df.write.format("delta").mode(mode).option("mergeSchema", "true").save(path)

  override def exists(spark: SparkSession, path: String): Boolean =
    try { DeltaTable.forPath(spark, path); true } catch { case _: Exception => false }
}

// =============================================================================
// PARQUET
// =============================================================================

class ParquetFormat extends DataFormat {

  override def read(spark: SparkSession, path: String): DataFrame =
    spark.read.parquet(path)

  override def write(spark: SparkSession, df: DataFrame, path: String, mode: String): Unit =
    df.write.mode(mode).parquet(path)

  override def exists(spark: SparkSession, path: String): Boolean =
    try { spark.read.parquet(path).head(1); true } catch { case _: Exception => false }
}

// =============================================================================
// APACHE ICEBERG
// =============================================================================

class IcebergFormat extends DataFormat {

  override def read(spark: SparkSession, path: String): DataFrame =
    spark.read.format("iceberg").load(path)

  override def write(spark: SparkSession, df: DataFrame, path: String, mode: String): Unit =
    df.write.format("iceberg").mode(mode).save(path)

  override def exists(spark: SparkSession, path: String): Boolean =
    try { spark.read.format("iceberg").load(path).head(1); true } catch { case _: Exception => false }
}
