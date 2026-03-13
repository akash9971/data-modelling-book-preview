package com.analytics.writer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.expressions.Window
import com.analytics.format.{DataFormat, DeltaFormat}
import io.delta.tables._

/**
 * ModelWriter — defines how a model's data gets written.
 * Implementations are created by WriterFactory.
 */
trait ModelWriter {
  def write(
    spark: SparkSession,
    newData: DataFrame,
    path: String,
    format: DataFormat,
    primaryKeys: List[String],
    scd2Columns: List[String]
  ): Unit
}

// =============================================================================
// FLAT WRITER — Simple overwrite, no merge logic
// =============================================================================

class FlatWriter extends ModelWriter {

  override def write(
    spark: SparkSession,
    newData: DataFrame,
    path: String,
    format: DataFormat,
    primaryKeys: List[String],
    scd2Columns: List[String]
  ): Unit = {
    format.write(spark, newData, path, "overwrite")
    println(s"    Flat write completed")
  }
}

// =============================================================================
// SCD TYPE 1 WRITER — Upsert: match on PK, overwrite non-key columns
// Delta: native MERGE
// Parquet/Iceberg: read-union-dedup-overwrite
// =============================================================================

class SCDType1Writer extends ModelWriter {

  override def write(
    spark: SparkSession,
    newData: DataFrame,
    path: String,
    format: DataFormat,
    primaryKeys: List[String],
    scd2Columns: List[String]
  ): Unit = {

    require(primaryKeys.nonEmpty, s"Primary keys required for SCD Type 1. Path: $path")

    // First run — no existing table
    if (!format.exists(spark, path)) {
      format.write(spark, newData, path, "overwrite")
      println(s"    Created new SCD Type 1 table at $path")
      return
    }

    // Subsequent runs
    format match {
      case _: DeltaFormat => mergeDelta(spark, newData, path, primaryKeys)
      case _              => mergeGeneric(spark, newData, path, format, primaryKeys)
    }
  }

  /** Delta: native MERGE — match on PKs, update non-keys, insert new */
  private def mergeDelta(
    spark: SparkSession,
    newData: DataFrame,
    path: String,
    primaryKeys: List[String]
  ): Unit = {
    val deltaTable = DeltaTable.forPath(spark, path)
    val mergeCondition = primaryKeys.map(k => s"target.$k = source.$k").mkString(" AND ")

    val updateMap = newData.columns
      .filterNot(primaryKeys.contains)
      .map(c => c -> s"source.$c").toMap

    val insertMap = newData.columns.map(c => c -> s"source.$c").toMap

    deltaTable.as("target")
      .merge(newData.as("source"), mergeCondition)
      .whenMatched().updateExpr(updateMap)
      .whenNotMatched().insertExpr(insertMap)
      .execute()

    println(s"    SCD Type 1 Delta MERGE completed")
  }

  /** Parquet/Iceberg: read → union → deduplicate (keep new) → overwrite */
  private def mergeGeneric(
    spark: SparkSession,
    newData: DataFrame,
    path: String,
    format: DataFormat,
    primaryKeys: List[String]
  ): Unit = {
    val existing = format.read(spark, path)

    val newWithFlag = newData.withColumn("__src_priority", lit(1))
    val existingWithFlag = existing.withColumn("__src_priority", lit(0))

    val window = Window
      .partitionBy(primaryKeys.map(col): _*)
      .orderBy(col("__src_priority").desc)

    val merged = newWithFlag.unionByName(existingWithFlag)
      .withColumn("__rn", row_number().over(window))
      .filter(col("__rn") === 1)
      .drop("__src_priority", "__rn")

    format.write(spark, merged, path, "overwrite")
    println(s"    SCD Type 1 generic merge completed")
  }
}

// =============================================================================
// SCD TYPE 2 WRITER — Full history tracking with effective dates
//
// Adds columns: effective_start_date, effective_end_date, current_flag
//
// Delta: Single MERGE with null-key trick (atomic expire + insert)
//   - For each CHANGED record, staged dataset gets 2 rows:
//       merge_key = PK value → MATCHES old row   → EXPIRE (flag=N, end_date=now)
//       merge_key = null     → NO MATCH           → INSERT (new current row)
//   - For each NEW record, staged dataset gets 1 row:
//       merge_key = null     → NO MATCH           → INSERT
//   - Why null works: SQL evaluates (target.pk = null) as FALSE always
//
// Parquet/Iceberg: read-process-overwrite (no native MERGE)
// =============================================================================

class SCDType2Writer extends ModelWriter {

  override def write(
    spark: SparkSession,
    newData: DataFrame,
    path: String,
    format: DataFormat,
    primaryKeys: List[String],
    scd2Columns: List[String]
  ): Unit = {
    import spark.implicits._

    require(primaryKeys.nonEmpty, s"Primary keys required for SCD Type 2. Path: $path")
    require(scd2Columns.nonEmpty, s"SCD2 columns required for SCD Type 2. Path: $path")

    val now = current_timestamp()

    // Add SCD2 system columns to incoming data
    val newDataWithSCD = newData
      .withColumn("effective_start_date", now)
      .withColumn("effective_end_date", lit(null).cast(TimestampType))
      .withColumn("current_flag", lit("Y"))

    // First run — table doesn't exist
    if (!format.exists(spark, path)) {
      format.write(spark, newDataWithSCD, path, "overwrite")
      println(s"    Created new SCD Type 2 table at $path")
      return
    }

    // Subsequent runs — detect changes
    val existingData = format.read(spark, path)

    // Join condition: match on primary keys
    val joinCond = primaryKeys.map(k =>
      newDataWithSCD(k) === existingData(k)
    ).reduce(_ && _)

    // Change condition: any tracked column differs
    val changeCond = scd2Columns.map { c =>
      (newDataWithSCD(c).isNull && existingData(c).isNotNull) ||
      (newDataWithSCD(c).isNotNull && existingData(c).isNull) ||
      (newDataWithSCD(c) =!= existingData(c))
    }.reduce(_ || _)

    // Changed records: exist in both sides, something in scd2Columns differs
    val changedRecords = newDataWithSCD.as("new")
      .join(existingData.as("old").filter($"current_flag" === "Y"), joinCond, "inner")
      .where(changeCond)
      .select($"new.*")

    // Brand new records: no match in existing active rows
    val brandNewRecords = newDataWithSCD.as("new")
      .join(existingData.as("old").filter($"current_flag" === "Y"), joinCond, "left_outer")
      .where($"old.${primaryKeys.head}".isNull)
      .select($"new.*")

    val changedCount = changedRecords.count()
    val newCount = brandNewRecords.count()
    println(s"    Changed: $changedCount | New: $newCount")

    if (changedCount == 0 && newCount == 0) {
      println(s"    No changes detected, skipping write")
      return
    }

    // Write using format-specific approach
    format match {
      case _: DeltaFormat =>
        writeDeltaSingleMerge(spark, path, changedRecords, brandNewRecords,
          primaryKeys, changedCount, newCount)
      case _ =>
        writeGeneric(spark, path, format, existingData, now,
          changedRecords, brandNewRecords, primaryKeys, changedCount, newCount)
    }

    println(s"    SCD Type 2 processing completed")
  }

  // ===========================================================================
  // DELTA: Single atomic MERGE with null-key trick
  //
  // Staged dataset example for Joinedmeeting changed + ChatMessage new:
  //
  //   merge_key       | msg_type      | msg_version | current_flag | action
  //   ────────────────┼───────────────┼─────────────┼──────────────┼────────
  //   Joinedmeeting   | Joinedmeeting | v5          | Y            | EXPIRE old (key matches)
  //   null            | Joinedmeeting | v5          | Y            | INSERT new (null never matches)
  //   null            | ChatMessage   | v1          | Y            | INSERT new (null never matches)
  //
  // MERGE ON target.msg_type = source.merge_key AND target.current_flag = 'Y'
  //   WHEN MATCHED     → UPDATE SET current_flag='N', effective_end_date=now()
  //   WHEN NOT MATCHED → INSERT (all columns except merge_key)
  // ===========================================================================

  private def writeDeltaSingleMerge(
    spark: SparkSession,
    path: String,
    changedRecords: DataFrame,
    brandNewRecords: DataFrame,
    primaryKeys: List[String],
    changedCount: Long,
    newCount: Long
  ): Unit = {

    val mergeKeyCol = "__merge_key"
    var stagedParts = List.empty[DataFrame]

    // Changed records → 2 rows each
    if (changedCount > 0) {
      // Row 1: merge_key = actual PK → will match target → EXPIRE old row
      val expireRows = changedRecords
        .withColumn(mergeKeyCol, col(primaryKeys.head))

      // Row 2: merge_key = null → won't match target → INSERT as new current
      val insertChangedRows = changedRecords
        .withColumn(mergeKeyCol, lit(null).cast(StringType))

      stagedParts = stagedParts :+ expireRows :+ insertChangedRows
    }

    // Brand new records → 1 row each
    if (newCount > 0) {
      val insertNewRows = brandNewRecords
        .withColumn(mergeKeyCol, lit(null).cast(StringType))

      stagedParts = stagedParts :+ insertNewRows
    }

    val stagedData = stagedParts.reduce(_ unionByName _)

    val totalStaged = stagedData.count()
    println(s"    Staged dataset: $totalStaged rows " +
      s"($changedCount expire + $changedCount changed-insert + $newCount new-insert)")

    // ---- Single atomic MERGE ----

    val deltaTable = DeltaTable.forPath(spark, path)

    // Match on merge_key (not the actual PK column) + current_flag
    val mergeCondition =
      s"target.${primaryKeys.head} = source.$mergeKeyCol AND target.current_flag = 'Y'"

    // Insert all columns EXCEPT the temporary merge_key
    val insertColumns = stagedData.columns.filterNot(_ == mergeKeyCol)
    val insertMap = insertColumns.map(c => c -> s"source.$c").toMap

    deltaTable.as("target")
      .merge(stagedData.as("source"), mergeCondition)
      .whenMatched()
      .updateExpr(Map(
        "effective_end_date" -> "current_timestamp()",
        "current_flag" -> "'N'"
      ))
      .whenNotMatched()
      .insertExpr(insertMap)
      .execute()

    println(s"    Single Delta MERGE completed: " +
      s"$changedCount expired + ${changedCount + newCount} inserted (1 atomic transaction)")
  }

  // ===========================================================================
  // PARQUET / ICEBERG: read-process-overwrite
  // Same logic but without native MERGE — rewrites entire table
  // ===========================================================================

  private def writeGeneric(
    spark: SparkSession,
    path: String,
    format: DataFormat,
    existingData: DataFrame,
    now: org.apache.spark.sql.Column,
    changedRecords: DataFrame,
    brandNewRecords: DataFrame,
    primaryKeys: List[String],
    changedCount: Long,
    newCount: Long
  ): Unit = {
    import spark.implicits._

    // Step 1: Process existing data — expire rows that have changes
    val processedExisting = if (changedCount > 0) {
      val changedKeys = changedRecords.select(primaryKeys.map(col): _*).distinct()

      // Rows to expire: active rows whose PK is in changedKeys
      val toExpire = existingData
        .join(changedKeys, primaryKeys, "left_semi")
        .filter($"current_flag" === "Y")
        .withColumn("effective_end_date", now)
        .withColumn("current_flag", lit("N"))

      // All other rows: history rows + unchanged active rows
      val untouched = existingData.except(
        existingData
          .join(changedKeys, primaryKeys, "left_semi")
          .filter($"current_flag" === "Y")
      )

      untouched.unionByName(toExpire)
    } else {
      existingData
    }

    // Step 2: Build all new rows to add (changed + brand new)
    val allNewRows = (changedCount > 0, newCount > 0) match {
      case (true, true)   => changedRecords.unionByName(brandNewRecords)
      case (true, false)  => changedRecords
      case (false, true)  => brandNewRecords
      case (false, false) => return // shouldn't reach here
    }

    // Step 3: Combine and overwrite
    val finalData = processedExisting.unionByName(allNewRows)
    format.write(spark, finalData, path, "overwrite")
    println(s"    SCD Type 2 generic write: $changedCount expired, ${changedCount + newCount} inserted")
  }
}
