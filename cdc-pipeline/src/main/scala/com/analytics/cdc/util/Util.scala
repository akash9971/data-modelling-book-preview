package com.analytics.cdc.util

import com.analytics.cdc.config.{ColumnMapping, DerivationRule}
import com.analytics.cdc.model.CdcColumns
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

object Util {

  def addContentHash(df: DataFrame): DataFrame = {
    val businessCols = df.columns.toSeq
    require(businessCols.nonEmpty, "Util.addContentHash: no columns available to build hash key")

    val contentExpr = concat_ws("|", businessCols.map(c => coalesce(col(c).cast("string"), lit("NULL"))): _*)
    df.withColumn(CdcColumns.HashKey, sha2(contentExpr, 256))
  }

  def applyMappings(df: DataFrame, mappings: List[ColumnMapping]): DataFrame = {
    if (mappings.isEmpty) return df

    val mappedSources = mappings.map(_.sourceName).toSet

    val renamedExprs = mappings.collect {
      case m if df.columns.contains(m.sourceName) && m.mask =>
        sha2(col(m.sourceName).cast(StringType), 256).as(m.targetName)
      case m if df.columns.contains(m.sourceName) =>
        col(m.sourceName).as(m.targetName)
    }

    val passthroughExprs = df.columns
      .filterNot(mappedSources.contains)
      .map(c => col(c))

    df.select((renamedExprs ++ passthroughExprs): _*)
  }

  def applyDerivations(df: DataFrame, derivations: List[DerivationRule]): DataFrame =
    derivations.foldLeft(df) { (acc, rule) =>
      acc.withColumn(rule.targetName, expr(rule.expression))
    }

  def withJobGroup[T](spark: SparkSession, groupId: String, description: String)(block: => T): T = {
    spark.sparkContext.setJobGroup(groupId, description, interruptOnCancel = true)
    try block
    finally spark.sparkContext.clearJobGroup()
  }
}
