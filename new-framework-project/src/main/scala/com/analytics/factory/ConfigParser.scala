package com.analytics.factory

import com.analytics.models._
import play.api.libs.json._
import scala.io.Source

/**
 * ConfigParser — reads JSON configuration files and produces typed case-class instances.
 *
 * v2 additions:
 *   parseModelJson  → ReadPath gains primaryKey (Optional[String])
 *                     Model gains hashColumns (Option[List[String]])
 *   parseConfigJson → ModelConfig gains stagePath and committedPath
 *   parseSemanticJson → SemanticModel gains sourceModelName and primaryKeys
 */
object ConfigParser {

  // ===========================================================================
  // SESSION TRACKING PIPELINE CONFIG
  // ===========================================================================

  /** Parses model.json → (read paths, model definitions).
   *
   *  read_paths is a JSON object keyed by view name; each entry may include an
   *  optional "primary_key" field.  Models may include an optional "hash_columns"
   *  array; when absent, HashKeyBuilder uses all columns. */
  def parseModelJson(path: String): (List[ReadPath], List[Model]) = {
    val json = Json.parse(Source.fromFile(path).getLines().mkString)

    val readPaths = (json \ "read_paths").as[JsObject].fields.map { case (viewName, obj) =>
      ReadPath(
        viewName   = viewName,
        path       = (obj \ "path").as[String],
        format     = (obj \ "format").asOpt[String].getOrElse("delta"),
        loadType   = (obj \ "load_type").asOpt[String].getOrElse("full"),
        primaryKey = (obj \ "primary_key").asOpt[String]
      )
    }.toList

    val models = (json \ "models").as[JsArray].value.map { m =>
      Model(
        name               = (m \ "name").as[String],
        modelType          = (m \ "type").asOpt[String].getOrElse("flat"),
        query              = (m \ "query").as[String],
        lineage            = (m \ "lineage").as[List[String]],
        primaryKeys        = (m \ "primary_keys").asOpt[List[String]].getOrElse(List.empty),
        updateTimestampCol = (m \ "update_timestamp_col").asOpt[String],
        scd2Columns        = (m \ "scd2_columns").asOpt[List[String]],
        hashColumns        = (m \ "hash_columns").asOpt[List[String]]
      )
    }.toList

    (readPaths, models)
  }

  /** Parses config.json → list of ModelConfig.
   *
   *  Each entry now requires stage_path and committed_path in addition to the
   *  existing write_path so the pipeline knows where to land staged output and
   *  where to look for the committed baseline. */
  def parseConfigJson(path: String): List[ModelConfig] = {
    val json = Json.parse(Source.fromFile(path).getLines().mkString)

    (json \ "attribute_mappings").as[JsArray].value.map { c =>
      val mappings = (c \ "mappings").as[JsArray].value.map { m =>
        AttributeMapping(
          sourceName = (m \ "source_name").as[String],
          targetName = (m \ "target_name").as[String],
          mask       = (m \ "mask").as[Boolean]
        )
      }.toList

      ModelConfig(
        modelName     = (c \ "model_name").as[String],
        modelType     = (c \ "model_type").asOpt[String].getOrElse("flat"),
        loadType      = (c \ "load_type").asOpt[String].getOrElse("full"),
        mappings      = mappings,
        writePath     = (c \ "write_path").as[String],
        stagePath     = (c \ "stage_path").as[String],
        committedPath = (c \ "committed_path").as[String],
        writeMode     = (c \ "write_mode").asOpt[String].getOrElse("overwrite"),
        writeFormat   = (c \ "write_format").asOpt[String].getOrElse("delta")
      )
    }.toList
  }

  // ===========================================================================
  // SEMANTIC LAYER PIPELINE CONFIG
  // ===========================================================================

  /** Parses semantic_model.json → (Snowflake config, source models, semantic models).
   *
   *  SemanticModel gains:
   *    source_model_name — links back to a ModelConfig name for delta/committed lookups
   *    primary_keys      — columns used in the Snowflake MERGE join condition */
  def parseSemanticJson(path: String): (SnowflakeConfig, List[SourceModel], List[SemanticModel]) = {
    val json = Json.parse(Source.fromFile(path).getLines().mkString)

    val sfJson = (json \ "snowflake_config").as[JsObject]
    val sfConfig = SnowflakeConfig(
      url       = (sfJson \ "url").as[String],
      database  = (sfJson \ "database").as[String],
      schema    = (sfJson \ "schema").as[String],
      warehouse = (sfJson \ "warehouse").as[String],
      role      = (sfJson \ "role").as[String]
    )

    val sourceModels = (json \ "source_models").as[JsObject].fields.map { case (name, obj) =>
      SourceModel(
        name   = name,
        path   = (obj \ "path").as[String],
        format = (obj \ "format").asOpt[String].getOrElse("delta")
      )
    }.toList

    val semanticModels = (json \ "semantic_models").as[JsArray].value.map { m =>
      SemanticModel(
        tableName        = (m \ "table_name").as[String],
        sourceModelName  = (m \ "source_model_name").as[String],
        primaryKeys      = (m \ "primary_keys").asOpt[List[String]].getOrElse(List.empty),
        query            = (m \ "query").as[String],
        viewRequiredFlag = (m \ "view_required_flag").as[Boolean],
        viewQuery        = (m \ "view_query").asOpt[String]
      )
    }.toList

    (sfConfig, sourceModels, semanticModels)
  }
}
