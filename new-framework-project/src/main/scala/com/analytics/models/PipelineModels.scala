package com.analytics.models

// =============================================================================
// SESSION TRACKING PIPELINE v2 — MODELS
// =============================================================================

/** Source data read path configuration.
 *  primaryKey: if present, signals this source has a natural PK usable for
 *  incremental watermarking and downstream hash comparisons. If absent the
 *  reader falls back to a full dataset load regardless of load_type. */
case class ReadPath(
  viewName:   String,
  path:       String,
  format:     String,
  loadType:   String,
  primaryKey: Option[String] = None
)

/** SQL model definition from model.json.
 *  hashColumns: explicit list of columns whose values form the row-level hash
 *  key written to the stage folder. If absent, every non-system column is used
 *  so that any field change is captured. */
case class Model(
  name:              String,
  modelType:         String,
  query:             String,
  lineage:           List[String],
  primaryKeys:       List[String],
  updateTimestampCol: Option[String]    = None,
  scd2Columns:       Option[List[String]] = None,
  hashColumns:       Option[List[String]] = None
)

/** Column mapping with optional SHA-256 masking */
case class AttributeMapping(
  sourceName: String,
  targetName: String,
  mask:       Boolean
)

/** Write configuration for a model from config.json.
 *  stagePath:     landing zone for each run's model output + hash keys.
 *  committedPath: promoted, durable snapshot — the previous run's accepted state.
 *                 Populated after a successful Snowflake write; used as the
 *                 delta baseline on the next incremental run. */
case class ModelConfig(
  modelName:    String,
  modelType:    String,
  loadType:     String,
  mappings:     List[AttributeMapping],
  writePath:    String,
  stagePath:    String,
  committedPath: String,
  writeMode:    String,
  writeFormat:  String
)

// =============================================================================
// SEMANTIC LAYER PIPELINE v2 — MODELS
// =============================================================================

/** Snowflake connection configuration */
case class SnowflakeConfig(
  url:       String,
  database:  String,
  schema:    String,
  warehouse: String,
  role:      String
)

/** Curated-layer source reference consumed by the semantic pipeline */
case class SourceModel(
  name:   String,
  path:   String,
  format: String
)

/** Semantic model definition — Snowflake table + optional companion view.
 *  sourceModelName: links back to the ModelConfig whose committed/stage paths
 *                   are used for delta detection before the Snowflake upsert.
 *  primaryKeys:     columns used in the Snowflake MERGE join condition. */
case class SemanticModel(
  tableName:       String,
  sourceModelName: String,
  primaryKeys:     List[String],
  query:           String,
  viewRequiredFlag: Boolean,
  viewQuery:       Option[String]
)
