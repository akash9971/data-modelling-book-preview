package com.analytics

import com.analytics.factory.{ConfigParser, FormatFactory}
import com.analytics.models.SnowflakeConfig
import com.analytics.stage.{DeltaDetector, StageManager}
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.sql.{Connection, DriverManager, Statement}

/**
 * SEMANTIC LAYER PIPELINE v2 — curated → Snowflake with delta-only upserts
 *
 * Run modes
 * ─────────
 * FULL LOAD  (committed does not yet exist for a model's source):
 *   1. Read from curated write_path (committed not available yet).
 *   2. Run semantic SQL.
 *   3. Write ALL records to Snowflake (overwrite).
 *   4. Promote stage → committed so future runs have a delta baseline.
 *
 * INCREMENTAL LOAD (committed exists):
 *   1. Read from committedPath into temp views.
 *   2. Run semantic SQL over those views.
 *   3. Compare stage vs committed on __hash_key → isolate delta records.
 *   4. Filter semantic output to only records whose source PK appears in delta.
 *   5. MERGE (upsert) only those records into the Snowflake table.
 *   6. Promote stage → committed so the baseline reflects everything shipped.
 *
 * Usage:
 *   spark-submit --class com.analytics.SemanticLayerPipeline \
 *     --conf spark.snowflake.user=USER --conf spark.snowflake.password=PASS \
 *     pipeline.jar semantic_model.json config.json
 */
object SemanticLayerPipeline {

  def main(args: Array[String]): Unit = {
    require(args.length == 2,
      "Usage: SemanticLayerPipeline <semantic_model.json> <config.json>")

    val spark = SparkSession.builder()
      .appName("Semantic Layer Pipeline v2")
      .config("spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension," +
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.iceberg_catalog.type", "hadoop")
      .getOrCreate()

    try {
      val sfUser     = spark.conf.get("spark.snowflake.user",     sys.env.getOrElse("SF_USER",     ""))
      val sfPassword = spark.conf.get("spark.snowflake.password", sys.env.getOrElse("SF_PASSWORD", ""))
      require(sfUser.nonEmpty && sfPassword.nonEmpty,
        "Set Snowflake credentials via spark.snowflake.user/password or SF_USER/SF_PASSWORD")

      println("=" * 80)
      println("SEMANTIC LAYER PIPELINE v2 — STARTING")
      println("=" * 80)

      val (sfConfig, sourceModels, semanticModels) = ConfigParser.parseSemanticJson(args(0))
      val modelConfigs = ConfigParser.parseConfigJson(args(1))
      val configMap    = modelConfigs.map(c => c.modelName -> c).toMap

      println(s"\nSnowflake Target: ${sfConfig.database}.${sfConfig.schema}")
      println(s"Source Models:   ${sourceModels.size}")
      println(s"Semantic Models: ${semanticModels.size}")

      val sfOptions = buildSfOptions(sfConfig, sfUser, sfPassword)

      // ======================================================================
      // STEP 1: Load curated source models into Spark temp views
      //
      // Prefer committedPath when it exists; fall back to write_path for models
      // that have never been promoted (e.g., first-ever pipeline run).
      // ======================================================================
      println("\n" + "=" * 80)
      println("STEP 1: Loading Curated Source Models")
      println("=" * 80)

      sourceModels.foreach { sm =>
        println(s"\nLoading: ${sm.name}")

        val format = FormatFactory.create(sm.format)
        val cfg    = configMap.get(sm.name)

        val (loadPath, label) = cfg match {
          case Some(c) if format.exists(spark, c.committedPath) =>
            (c.committedPath, "committed")
          case _ =>
            (sm.path, "curated write_path (committed not yet available)")
        }

        println(s"  Path [${label}]: $loadPath")
        val df = format.read(spark, loadPath)
        df.createOrReplaceTempView(sm.name)
        println(s"  Records: ${df.count()}")
        println(s"  Columns: ${df.columns.mkString(", ")}")
      }

      // ======================================================================
      // STEP 2: Execute semantic models → write to Snowflake
      // ======================================================================
      println("\n" + "=" * 80)
      println("STEP 2: Building Semantic Models and Writing to Snowflake")
      println("=" * 80)

      semanticModels.foreach { sm =>
        println("\n" + "-" * 60)
        println(s"Semantic Table:  ${sfConfig.database}.${sfConfig.schema}.${sm.tableName}")
        println(s"Source Model:    ${sm.sourceModelName}")
        println(s"Primary Keys:    ${sm.primaryKeys.mkString(", ")}")
        println(s"View Required:   ${sm.viewRequiredFlag}")

        val cfg          = configMap.get(sm.sourceModelName)
        val format       = cfg.map(c => FormatFactory.create(c.writeFormat))
        val stageManager = format.map(f => new StageManager(f))

        val committedExists = cfg.zip(stageManager).exists { case (c, sm2) =>
          sm2.committedExists(spark, c.committedPath)
        }

        // Run the semantic SQL against curated temp views already registered in Step 1
        println(s"\nExecuting query: ${sm.query.take(200)}...")
        val semanticDF = spark.sql(sm.query)
        println(s"  Records produced: ${semanticDF.count()}")

        if (!committedExists) {
          // -------------------------------------------------------------------
          // FULL LOAD: first run — write everything to Snowflake
          // -------------------------------------------------------------------
          println(s"\n  [FULL LOAD] Writing all records to Snowflake: ${sm.tableName}")
          writeToSnowflake(semanticDF, sfOptions, sm.tableName)
          println(s"  Written to Snowflake: ${sm.tableName}")

          // Promote stage → committed now that Snowflake write succeeded
          cfg.zip(stageManager).foreach { case (c, sm2) =>
            if (sm2.stageExists(spark, c.stagePath)) {
              sm2.promoteToCommitted(spark, c.stagePath, c.committedPath)
              println(s"  Stage promoted to committed: ${c.committedPath}")
            }
          }

        } else {
          // -------------------------------------------------------------------
          // INCREMENTAL LOAD: detect delta, upsert only changed records
          // -------------------------------------------------------------------
          println(s"\n  [INCREMENTAL LOAD] Detecting delta for: ${sm.tableName}")

          val (stageDF, committedDF) = cfg.zip(stageManager).map { case (c, sm2) =>
            (sm2.readStage(spark, c.stagePath), sm2.readCommitted(spark, c.committedPath))
          }.getOrElse(throw new IllegalStateException(
            s"Config or StageManager unavailable for model: ${sm.sourceModelName}"))

          val deltaDF    = DeltaDetector.detectDelta(stageDF, committedDF)
          val deltaCount = deltaDF.count()
          println(s"  Delta records detected: $deltaCount")

          if (deltaCount > 0 && sm.primaryKeys.nonEmpty) {
            // Filter semantic output to only records whose PKs appear in delta
            val deltaPKs = deltaDF.select(
              sm.primaryKeys.map(org.apache.spark.sql.functions.col): _*
            ).distinct()

            val deltaSemanticDF = semanticDF.join(deltaPKs, sm.primaryKeys, "inner")
            println(s"  Delta semantic records to upsert: ${deltaSemanticDF.count()}")

            upsertToSnowflake(spark, deltaSemanticDF, sfOptions, sm.tableName, sm.primaryKeys, sfConfig)
            println(s"  Delta upserted to Snowflake: ${sm.tableName}")
          } else if (deltaCount > 0) {
            // No PKs configured — overwrite (safe for dimension tables without a PK)
            println(s"  No primary keys for Snowflake MERGE — falling back to overwrite")
            writeToSnowflake(semanticDF, sfOptions, sm.tableName)
          } else {
            println(s"  No delta — Snowflake table unchanged")
          }

          // Promote stage → committed after successful Snowflake operation
          cfg.zip(stageManager).foreach { case (c, sm2) =>
            if (sm2.stageExists(spark, c.stagePath)) {
              sm2.promoteToCommitted(spark, c.stagePath, c.committedPath)
              println(s"  Stage promoted to committed: ${c.committedPath}")
            }
          }
        }

        // Create companion view in Snowflake if required
        if (sm.viewRequiredFlag && sm.viewQuery.isDefined) {
          println(s"\n  Creating Snowflake view...")
          executeSnowflakeSQL(sfOptions, sm.viewQuery.get, sfConfig)
          println(s"  View created/replaced successfully")
        }
      }

      // ======================================================================
      // STEP 3: Validation — row counts in Snowflake
      // ======================================================================
      println("\n" + "=" * 80)
      println("STEP 3: Validation")
      println("=" * 80)

      semanticModels.foreach { sm =>
        val countDf = readFromSnowflake(spark, sfOptions,
          s"SELECT COUNT(*) as cnt FROM ${sfConfig.database}.${sfConfig.schema}.${sm.tableName}")
        val count = countDf.collect()(0).getLong(0)
        println(s"  ${sm.tableName}: $count records in Snowflake")
      }

      println("\n" + "=" * 80)
      println("SEMANTIC LAYER PIPELINE v2 — COMPLETED SUCCESSFULLY")
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
  // SNOWFLAKE I/O
  // ==========================================================================

  private def buildSfOptions(
    sfConfig:   SnowflakeConfig,
    sfUser:     String,
    sfPassword: String
  ): Map[String, String] = Map(
    "sfURL"       -> sfConfig.url,
    "sfUser"      -> sfUser,
    "sfPassword"  -> sfPassword,
    "sfDatabase"  -> sfConfig.database,
    "sfSchema"    -> sfConfig.schema,
    "sfWarehouse" -> sfConfig.warehouse,
    "sfRole"      -> sfConfig.role
  )

  /** Full overwrite — used on first-run full load. */
  def writeToSnowflake(df: DataFrame, sfOptions: Map[String, String], tableName: String): Unit =
    df.write
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("dbtable", tableName)
      .mode("overwrite")
      .save()

  /**
   * Delta upsert — writes delta rows into a short-lived staging table in Snowflake,
   * then issues a MERGE statement to atomically update matched rows and insert new ones.
   * The staging table is dropped after the merge.
   */
  def upsertToSnowflake(
    spark:       SparkSession,
    deltaDF:     DataFrame,
    sfOptions:   Map[String, String],
    tableName:   String,
    primaryKeys: List[String],
    sfConfig:    SnowflakeConfig
  ): Unit = {
    val stagingTable = s"${tableName}_DELTA_STG_${System.currentTimeMillis()}"

    // Write delta into a temporary staging table
    deltaDF.write
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("dbtable", stagingTable)
      .mode("overwrite")
      .save()

    val joinClause    = primaryKeys.map(pk => s"t.$pk = s.$pk").mkString(" AND ")
    val nonPkCols     = deltaDF.columns.filterNot(primaryKeys.contains)
    val updateClause  = nonPkCols.map(c => s"t.$c = s.$c").mkString(", ")
    val insertCols    = deltaDF.columns.mkString(", ")
    val insertVals    = deltaDF.columns.map(c => s"s.$c").mkString(", ")

    val mergeSQL =
      s"""MERGE INTO $tableName t
         |USING $stagingTable s
         |ON ($joinClause)
         |WHEN MATCHED THEN UPDATE SET $updateClause
         |WHEN NOT MATCHED THEN INSERT ($insertCols) VALUES ($insertVals)
         |""".stripMargin

    executeSnowflakeSQL(sfOptions, mergeSQL, sfConfig)
    executeSnowflakeSQL(sfOptions, s"DROP TABLE IF EXISTS $stagingTable", sfConfig)
  }

  /** Read from Snowflake via arbitrary SQL (used for validation COUNT queries). */
  def readFromSnowflake(spark: SparkSession, sfOptions: Map[String, String], query: String): DataFrame =
    spark.read
      .format("net.snowflake.spark.snowflake")
      .options(sfOptions)
      .option("query", query)
      .load()

  /** Execute DDL or DML in Snowflake via JDBC (used for CREATE VIEW and MERGE). */
  def executeSnowflakeSQL(sfOptions: Map[String, String], sql: String, sfConfig: SnowflakeConfig): Unit = {
    val baseUrl  = sfOptions("sfURL").replace("https://", "").replace("http://", "").stripSuffix("/")
    val jdbcUrl  =
      s"jdbc:snowflake://$baseUrl/?db=${sfConfig.database}&schema=${sfConfig.schema}" +
      s"&warehouse=${sfConfig.warehouse}&role=${sfConfig.role}"

    var connection: Connection = null
    var statement: Statement   = null

    try {
      connection = DriverManager.getConnection(jdbcUrl, sfOptions("sfUser"), sfOptions("sfPassword"))
      statement  = connection.createStatement()
      statement.execute(s"USE WAREHOUSE ${sfConfig.warehouse}")
      statement.execute(s"USE DATABASE ${sfConfig.database}")
      statement.execute(s"USE SCHEMA ${sfConfig.schema}")
      statement.execute(sql)
      println(s"    JDBC SQL executed successfully")
    } catch {
      case e: Exception =>
        println(s"    JDBC SQL Error: ${e.getMessage}")
        throw e
    } finally {
      if (statement  != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}
