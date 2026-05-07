package com.analytics

import com.analytics.factory.{ConfigParser, FormatFactory, WriterFactory}
import com.analytics.stage.{DeltaDetector, HashKeyBuilder, StageManager}
import com.analytics.util.{LineageResolver, MappingTransformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * SESSION TRACKING PIPELINE v2 — raw → curated with stage/committed delta tracking
 *
 * Run modes
 * ─────────
 * FULL LOAD  (first run OR load_type = "full"):
 *   1. Read full source dataset.
 *   2. Execute SQL model.
 *   3. Apply column mappings + PII masking.
 *   4. Add hash key (all columns or explicit hash_columns list) and write to stage/.
 *   5. Write full mapped output to curated write_path via ModelWriter.
 *   6. Register curated table as temp view for downstream models.
 *   NOTE: stage → committed promotion happens in SemanticLayerPipeline
 *         after a successful Snowflake write, ensuring committed only ever
 *         reflects data that is confirmed delivered end-to-end.
 *
 * INCREMENTAL LOAD (subsequent runs where committed already exists):
 *   1. Read incremental source data (timestamp filter; PK logged for traceability).
 *   2. Execute SQL model on the incremental slice.
 *   3. Apply column mappings + PII masking.
 *   4. Add hash key and write to stage/ (overwrite — stage is run-scoped).
 *   5. Read committed/ (prior run's accepted state).
 *   6. DeltaDetector: left-anti join on __hash_key → delta records (new or changed).
 *   7. If delta > 0:
 *        a. Write delta (without hash columns) to curated write_path via ModelWriter.
 *        b. Merge delta (with hash columns) into committed/ so the baseline advances.
 *   8. Read full curated table back and register as temp view for downstream models.
 *
 * Usage:
 *   spark-submit --class com.analytics.SessionTrackingPipeline \
 *     pipeline.jar model.json config.json
 */
object SessionTrackingPipeline {

  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Usage: SessionTrackingPipeline <model.json> <config.json>")

    val spark = SparkSession.builder()
      .appName("Session Tracking Pipeline v2")
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension," +
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
      .getOrCreate()

    try {
      println("=" * 80)
      println("SESSION TRACKING PIPELINE v2 — STARTING")
      println("=" * 80)

      val (readPaths, models) = ConfigParser.parseModelJson(args(0))
      val modelConfigs        = ConfigParser.parseConfigJson(args(1))
      val configMap           = modelConfigs.map(c => c.modelName -> c).toMap

      // ======================================================================
      // STEP 1: Load source data into Spark temp views
      // ======================================================================
      println("\n" + "=" * 80)
      println("STEP 1: Loading Source Data")
      println("=" * 80)

      readPaths.foreach { rp =>
        println(s"\nLoading: ${rp.viewName}")
        println(s"  Path:      ${rp.path}")
        println(s"  Format:    ${rp.format.toUpperCase}")
        println(s"  Load Type: ${rp.loadType}")
        rp.primaryKey.foreach(pk => println(s"  Primary Key: $pk"))

        val format = FormatFactory.create(rp.format)

        // If load_type is incremental AND a primaryKey is declared, use it to
        // annotate the incremental read (watermark-based filter on timestamp col).
        // If no primaryKey is present the reader performs a simple full load.
        val df: DataFrame = rp.loadType.toLowerCase match {
          case "incremental" if rp.primaryKey.isDefined =>
            format.readIncremental(spark, rp.path, rp.primaryKey)
          case "incremental" =>
            println(s"  No primary_key declared — full dataset load")
            format.read(spark, rp.path)
          case _ =>
            format.read(spark, rp.path)
        }

        df.createOrReplaceTempView(rp.viewName)
        println(s"  Records Loaded: ${df.count()}")
      }

      // ======================================================================
      // STEP 2: Execute models in lineage order
      // ======================================================================
      println("\n" + "=" * 80)
      println("STEP 2: Executing Models in Lineage Order")
      println("=" * 80)

      val sortedModels = LineageResolver.resolve(models)

      sortedModels.foreach { model =>
        println("\n" + "-" * 60)
        println(s"Model:    ${model.name}")
        println(s"Type:     ${model.modelType.toUpperCase}")
        println(s"Lineage:  ${if (model.lineage.isEmpty) "None" else model.lineage.mkString(", ")}")
        model.hashColumns.foreach(hc => println(s"Hash Cols: ${hc.mkString(", ")}"))

        val config = configMap.getOrElse(model.name,
          throw new IllegalStateException(s"No write config for model: ${model.name}"))

        val format       = FormatFactory.create(config.writeFormat)
        val writer       = WriterFactory.create(model.modelType)
        val stageManager = new StageManager(format)

        // ----- Execute SQL model ------------------------------------------
        println(s"\nQuery: ${model.query.take(200)}...")
        val rawDF = spark.sql(model.query)
        println(s"  Records produced: ${rawDF.count()}")

        // ----- Apply column mappings + PII masking ------------------------
        val mappedDF = MappingTransformer.apply(rawDF, config.mappings)

        // Translate source-named PKs / SCD2 cols to their target names
        val mappedPKs  = model.primaryKeys.map(pk => MappingTransformer.getMappedName(pk, config.mappings))
        val mappedSCD2 = model.scd2Columns.getOrElse(List.empty)
          .map(c => MappingTransformer.getMappedName(c, config.mappings))

        // hash_columns are defined in source-name space; translate to target names
        val mappedHashCols: Option[List[String]] = model.hashColumns.map(cols =>
          cols.map(c => MappingTransformer.getMappedName(c, config.mappings))
        )

        val isFirstRun = !stageManager.committedExists(spark, config.committedPath)
        val isFullLoad = config.loadType.toLowerCase == "full"

        if (isFirstRun || isFullLoad) {
          runFullLoad(spark, model.name, mappedDF, config.writePath, config.stagePath,
            format, writer, mappedPKs, mappedSCD2, mappedHashCols, stageManager)
        } else {
          runIncrementalLoad(spark, model.name, mappedDF, config.writePath, config.stagePath,
            config.committedPath, format, writer, mappedPKs, mappedSCD2, mappedHashCols, stageManager)
        }

        // Read full curated table back so downstream model SQL can reference it
        val fullCurated = format.read(spark, config.writePath)
        fullCurated.createOrReplaceTempView(model.name)
        println(s"  Temp view '${model.name}' registered (${fullCurated.count()} total records)")
        println("\nSchema:")
        fullCurated.printSchema()
      }

      println("\n" + "=" * 80)
      println("SESSION TRACKING PIPELINE v2 — COMPLETED SUCCESSFULLY")
      println("=" * 80)

    } catch {
      case e: Exception =>
        println(s"\nERROR: ${e.getMessage}")
        e.printStackTrace()
        sys.exit(1)
    } finally {
      spark.stop()
    }
  }

  // ==========================================================================
  // FULL LOAD
  // ==========================================================================

  /**
   * Full load path — used on first ever run, or whenever load_type = "full".
   *
   * Writes to stage (with hash keys) and to the curated write path.
   * Does NOT promote stage → committed here; that happens in SemanticLayerPipeline
   * after a confirmed Snowflake write so committed always reflects delivered data.
   */
  private def runFullLoad(
    spark:        SparkSession,
    modelName:    String,
    mappedDF:     DataFrame,
    writePath:    String,
    stagePath:    String,
    format:       com.analytics.format.DataFormat,
    writer:       com.analytics.writer.ModelWriter,
    primaryKeys:  List[String],
    scd2Columns:  List[String],
    hashCols:     Option[List[String]],
    stageManager: StageManager
  ): Unit = {
    println(s"\n  [FULL LOAD] $modelName")

    // Write model output + hash key to stage
    stageManager.writeToStage(spark, mappedDF, stagePath, hashCols)

    // Write full dataset to curated write path via the model's writer strategy
    writer.write(spark, mappedDF, writePath, format, primaryKeys, scd2Columns)
    println(s"  Written to curated: $writePath")
    println(s"  Stage at $stagePath — will be promoted to committed after Snowflake write")
  }

  // ==========================================================================
  // INCREMENTAL LOAD
  // ==========================================================================

  /**
   * Incremental load path — runs when committed already exists.
   *
   * 1. Writes incremental model output (with hash keys) to stage/.
   * 2. Compares stage vs committed on __hash_key to find new/changed records.
   * 3. Writes only delta records to the curated write path.
   * 4. Merges delta (with hash columns) into committed so the baseline advances.
   */
  private def runIncrementalLoad(
    spark:         SparkSession,
    modelName:     String,
    mappedDF:      DataFrame,
    writePath:     String,
    stagePath:     String,
    committedPath: String,
    format:        com.analytics.format.DataFormat,
    writer:        com.analytics.writer.ModelWriter,
    primaryKeys:   List[String],
    scd2Columns:   List[String],
    hashCols:      Option[List[String]],
    stageManager:  StageManager
  ): Unit = {
    println(s"\n  [INCREMENTAL LOAD] $modelName")

    // Write incremental batch output + hash keys to stage
    val stageDF = stageManager.writeToStage(spark, mappedDF, stagePath, hashCols)

    // Read the committed baseline from the previous successful run
    val committedDF = stageManager.readCommitted(spark, committedPath)

    // Delta = rows in stage whose hash key is absent from committed
    val deltaDF    = DeltaDetector.detectDelta(stageDF, committedDF)
    val deltaCount = deltaDF.count()
    println(s"  Delta records (new/changed): $deltaCount")

    if (deltaCount > 0) {
      // Strip hash system columns before writing to curated layer
      val deltaForCurated = deltaDF.drop(HashKeyBuilder.HASH_KEY_COL, HashKeyBuilder.HASH_DATE_COL)

      // Upsert only delta records into the curated write path
      writer.write(spark, deltaForCurated, writePath, format, primaryKeys, scd2Columns)
      println(s"  Delta written to curated: $writePath")

      // Advance committed baseline: merge delta rows (with hash cols) in
      stageManager.mergeDeltaIntoCommitted(spark, deltaDF, committedPath, primaryKeys)
    } else {
      println(s"  No delta detected — curated and committed unchanged")
    }
  }
}
