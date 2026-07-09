package com.analytics.cdc.config

import play.api.libs.json._

import scala.io.Source

case class ColumnMapping(sourceName: String, targetName: String, mask: Boolean)

case class DerivationRule(targetName: String, expression: String)

case class TableConfig(
  modelName:   String,
  lmoPath:     String,
  emoPath:     String,
  primaryKeys: List[String],
  filter:      Option[String],
  mappings:    List[ColumnMapping],
  derivations: List[DerivationRule]
)

case class CdcAppConfig(
  cdcStrategy: String,
  tables:      List[TableConfig]
)

object CdcConfigParser {

  def parse(path: String): CdcAppConfig = {
    val source = Source.fromFile(path)
    try {
      val json = Json.parse(source.getLines().mkString)
      parseJson(json)
    } finally {
      source.close()
    }
  }

  def parseJson(json: JsValue): CdcAppConfig = {
    val strategy = (json \ "cdc_strategy").asOpt[String].getOrElse("hash_based")

    val tables = (json \ "tables").as[JsArray].value.map(parseTable).toList

    CdcAppConfig(strategy, tables)
  }

  private def parseTable(t: JsValue): TableConfig = {
    val mappings = (t \ "mappings").asOpt[JsArray]
      .map(_.value.map(parseMapping).toList)
      .getOrElse(List.empty)

    val derivations = (t \ "derivations").asOpt[JsArray]
      .map(_.value.map(parseDerivation).toList)
      .getOrElse(List.empty)

    TableConfig(
      modelName   = (t \ "model_name").as[String],
      lmoPath     = (t \ "lmo_path").as[String],
      emoPath     = (t \ "emo_path").as[String],
      primaryKeys = (t \ "primary_keys").as[List[String]],
      filter      = (t \ "filter").asOpt[String],
      mappings    = mappings,
      derivations = derivations
    )
  }

  private def parseMapping(m: JsValue): ColumnMapping = ColumnMapping(
    sourceName = (m \ "source_name").as[String],
    targetName = (m \ "target_name").as[String],
    mask       = (m \ "mask").asOpt[Boolean].getOrElse(false)
  )

  private def parseDerivation(d: JsValue): DerivationRule = DerivationRule(
    targetName = (d \ "target_name").as[String],
    expression = (d \ "expression").as[String]
  )
}
