package com.analytics.cdc.format

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataFormat {
  def read(spark: SparkSession, path: String): DataFrame
  def write(spark: SparkSession, df: DataFrame, path: String, mode: String): Unit
  def exists(spark: SparkSession, path: String): Boolean
}

class ParquetFormat extends DataFormat {

  override def read(spark: SparkSession, path: String): DataFrame =
    spark.read.parquet(path)

  override def write(spark: SparkSession, df: DataFrame, path: String, mode: String): Unit =
    df.write.mode(mode).parquet(path)

  override def exists(spark: SparkSession, path: String): Boolean =
    try {
      spark.read.parquet(path).head(1)
      true
    } catch {
      case _: Exception => false
    }
}

class DeltaFormat extends DataFormat {

  override def read(spark: SparkSession, path: String): DataFrame =
    spark.read.format("delta").load(path)

  override def write(spark: SparkSession, df: DataFrame, path: String, mode: String): Unit =
    df.write
      .format("delta")
      .mode(mode)
      .option("mergeSchema", "true")
      .save(path)

  override def exists(spark: SparkSession, path: String): Boolean =
    try {
      DeltaTable.forPath(spark, path)
      true
    } catch {
      case _: Exception => false
    }
}
