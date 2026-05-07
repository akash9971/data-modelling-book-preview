package com.analytics.writer

import com.analytics.format.{DataFormat, DeltaFormat}
import io.delta.tables._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

/**
 * ModelWriter — defines how a model's data gets written to the curated write path.
 *
 * In v2 this writer always operates on the CURATED write path, not on stage or
 * committed.  On incremental runs the pipeline calls it only with the delta records
 * (already identified by DeltaDetector) so each writer variant naturally handles
 * a smaller, change-only payload.
 *
 * Implementations are created by WriterFactory.
 */
trait ModelWriter {
  def write(
    spark:       SparkSession,
    newData:     DataFrame,
    path:        String,
    format:      DataFormat,
    primaryKeys: List[String],
    scd2Columns: List[String]
  ): Unit
}

// =============================================================================
// FLAT WRITER — simple overwrite, no merge logic
// =============================================================================

class FlatWriter extends ModelWriter {

  override def write(
    spark:       SparkSession,
    newData:     DataFrame,
    path:        String,
    format:      DataFormat,
    primaryKeys: List[String],
    scd2Columns: List[String]
  ): Unit = {
    format.write(spark, newData, path, "overwrite")
    println(s"    Flat write completed")
  }
}

// =============================================================================
// SCD TYPE 1 WRITER — upsert: match on PK, overwrite non-key columns
// =============================================================================

class SCDType1Writer extends ModelWriter {

  override def write(
    spark:       SparkSession,
    newData:     DataFrame,
    path:        String,
    format:      DataFormat,
    primaryKeys: List[String],
    scd2Columns: List[String]
  ): Unit = {
    require(primaryKeys.nonEmpty, s"Primary keys required for SCD Type 1. Path: $path")

    if (!format.exists(spark, path)) {
      format.write(spark, newData, path, "overwrite")
      println(s"    Created new SCD Type 1 table at $path")
      return
    }

    format match {
      case _: DeltaFormat => mergeDelta(spark, newData, path, primaryKeys)
      case _              => mergeGeneric(spark, newData, path, format, primaryKeys)
    }
  }

  private def mergeDelta(
    spark:       SparkSession,
    newData:     DataFrame,
    path:        String,
    primaryKeys: List[String]
  ): Unit = {
    val deltaTable   = DeltaTable.forPath(spark, path)
    val mergeCondition = primaryKeys.map(k => s"target.$k = source.$k").mkString(" AND ")
    val updateMap    = newData.columns.filterNot(primaryKeys.contains).map(c => c -> s"source.$c").toMap
    val insertMap    = newData.columns.map(c => c -> s"source.$c").toMap

    deltaTable.as("target")
      .merge(newData.as("source"), mergeCondition)
      .whenMatched().updateExpr(updateMap)
      .whenNotMatched().insertExpr(insertMap)
      .execute()

    println(s"    SCD Type 1 Delta MERGE completed")
  }

  private def mergeGeneric(
    spark:       SparkSession,
    newData:     DataFrame,
    path:        String,
    format:      DataFormat,
    primaryKeys: List[String]
  ): Unit = {
    val existing        = format.read(spark, path)
    val newWithFlag      = newData.withColumn("__src_priority", lit(1))
    val existingWithFlag = existing.withColumn("__src_priority", lit(0))
    val window          = Window.partitionBy(primaryKeys.map(col): _*).orderBy(col("__src_priority").desc)

    val merged = newWithFlag.unionByName(existingWithFlag)
      .withColumn("__rn", row_number().over(window))
      .filter(col("__rn") === 1)
      .drop("__src_priority", "__rn")

    format.write(spark, merged, path, "overwrite")
    println(s"    SCD Type 1 generic merge (read-union-dedup-overwrite) completed")
  }
}

// =============================================================================
// SCD TYPE 2 WRITER — full history tracking with effective dates
// =============================================================================

class SCDType2Writer extends ModelWriter {

  override def write(
    spark:       SparkSession,
    newData:     DataFrame,
    path:        String,
    format:      DataFormat,
    primaryKeys: List[String],
    scd2Columns: List[String]
  ): Unit = {
    import spark.implicits._

    require(primaryKeys.nonEmpty, s"Primary keys required for SCD Type 2. Path: $path")
    require(scd2Columns.nonEmpty, s"SCD2 columns required for SCD Type 2. Path: $path")

    val now = current_timestamp()

    val newDataWithSCD = newData
      .withColumn("effective_start_date", now)
      .withColumn("effective_end_date", lit(null).cast(TimestampType))
      .withColumn("current_flag", lit("Y"))

    if (!format.exists(spark, path)) {
      format.write(spark, newDataWithSCD, path, "overwrite")
      println(s"    Created new SCD Type 2 table at $path")
      return
    }

    val existingData = format.read(spark, path)

    val joinCond   = primaryKeys.map(k => newDataWithSCD(k) === existingData(k)).reduce(_ && _)
    val changeCond = scd2Columns.map { c =>
      (newDataWithSCD(c).isNull     && existingData(c).isNotNull) ||
      (newDataWithSCD(c).isNotNull  && existingData(c).isNull)    ||
      (newDataWithSCD(c) =!= existingData(c))
    }.reduce(_ || _)

    val changedRecords = newDataWithSCD.as("new")
      .join(existingData.as("old").filter($"current_flag" === "Y"), joinCond, "inner")
      .where(changeCond)
      .select($"new.*")

    val changedCount = changedRecords.count()
    println(s"    Changed records detected: $changedCount")

    val newRecords = newDataWithSCD.as("new")
      .join(existingData.as("old").filter($"current_flag" === "Y"), joinCond, "left_outer")
      .where($"old.${primaryKeys.head}".isNull || changeCond)
      .select($"new.*")

    val insertCount = newRecords.count()

    format match {
      case _: DeltaFormat =>
        writeDelta(spark, path, changedRecords, newRecords, primaryKeys, changedCount, insertCount)
      case _ =>
        writeGeneric(spark, path, format, existingData, now,
          changedRecords, newRecords, primaryKeys, changedCount, insertCount)
    }

    println(s"    SCD Type 2 processing completed")
  }

  private def writeDelta(
    spark:         SparkSession,
    path:          String,
    changedRecords: DataFrame,
    newRecords:     DataFrame,
    primaryKeys:   List[String],
    changedCount:  Long,
    insertCount:   Long
  ): Unit = {
    if (changedCount > 0) {
      val deltaTable = DeltaTable.forPath(spark, path)
      val mergeCond  = primaryKeys.map(k => s"target.$k = source.$k").mkString(" AND ")

      deltaTable.as("target")
        .merge(
          changedRecords.select(primaryKeys.map(col): _*).distinct().as("source"),
          s"$mergeCond AND target.current_flag = 'Y'"
        )
        .whenMatched()
        .updateExpr(Map(
          "effective_end_date" -> "current_timestamp()",
          "current_flag"       -> "'N'"
        ))
        .execute()

      println(s"    Expired $changedCount records (Delta MERGE)")
    }

    if (insertCount > 0) {
      newRecords.write.format("delta").mode("append").save(path)
      println(s"    Inserted $insertCount records (Delta APPEND)")
    }
  }

  private def writeGeneric(
    spark:         SparkSession,
    path:          String,
    format:        DataFormat,
    existingData:  DataFrame,
    now:           org.apache.spark.sql.Column,
    changedRecords: DataFrame,
    newRecords:    DataFrame,
    primaryKeys:   List[String],
    changedCount:  Long,
    insertCount:   Long
  ): Unit = {
    import spark.implicits._

    val processedExisting = if (changedCount > 0) {
      val changedKeys = changedRecords.select(primaryKeys.map(col): _*).distinct()

      val toExpire = existingData
        .join(changedKeys, primaryKeys, "left_semi")
        .filter($"current_flag" === "Y")
        .withColumn("effective_end_date", now)
        .withColumn("current_flag", lit("N"))

      val unchanged = existingData.except(
        existingData.join(changedKeys, primaryKeys, "left_semi").filter($"current_flag" === "Y")
      )

      unchanged.unionByName(toExpire)
    } else {
      existingData
    }

    val finalData = if (insertCount > 0) processedExisting.unionByName(newRecords) else processedExisting
    format.write(spark, finalData, path, "overwrite")
    println(s"    SCD Type 2 generic write completed: $changedCount expired, $insertCount inserted")
  }
}
