package com.analytics.cdc

import com.analytics.cdc.config.CdcConfigParser
import com.analytics.cdc.factory.Factory
import com.analytics.cdc.pipeline.TablePipeline
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Main {
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val parsedArgs = parseArgs(args)
    val configPath = parsedArgs.getOrElse("config", throw new IllegalArgumentException("--config is required"))
    val snapDate   = parsedArgs.getOrElse("snap-date", throw new IllegalArgumentException("--snap-date is required"))
    val force      = parsedArgs.get("force").contains("true")

    val appConfig = CdcConfigParser.parse(configPath)
    val strategy  = Factory.strategy(appConfig.cdcStrategy)

    val spark = SparkSession.builder()
      .appName("lmo-emo-cdc-pipeline")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    val pipeline = new TablePipeline(strategy)

    appConfig.tables.foreach { table =>
      try {
        pipeline.run(spark, table, snapDate, force)
      } catch {
        case e: Exception =>
          logger.error(s"[${table.modelName}] FAILED for snap_date=$snapDate", e)
          throw e
      }
    }
  }

  private[cdc] def parseArgs(args: Array[String]): Map[String, String] =
    args.collect {
      case arg if arg.startsWith("--") && arg.contains("=") =>
        val stripped = arg.stripPrefix("--")
        val idx      = stripped.indexOf("=")
        stripped.substring(0, idx) -> stripped.substring(idx + 1)
    }.toMap
}
