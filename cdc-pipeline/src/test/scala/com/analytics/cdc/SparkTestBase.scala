package com.analytics.cdc

import org.apache.spark.sql.SparkSession

trait SparkTestBase {
  val spark: SparkSession = SparkTestBase.sharedSpark

  def tempDir(prefix: String): String =
    java.nio.file.Files.createTempDirectory(prefix).toString
}

object SparkTestBase {
  lazy val sharedSpark: SparkSession = SparkSession.builder()
    .appName("cdc-pipeline-tests")
    .master("local[2]")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
}
