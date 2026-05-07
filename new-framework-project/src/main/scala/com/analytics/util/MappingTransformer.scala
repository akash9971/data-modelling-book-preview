package com.analytics.util

import com.analytics.models.AttributeMapping
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/** Applies column rename and optional SHA-256 masking based on config mappings. */
object MappingTransformer {

  /** Selects and transforms columns according to the mapping list.
   *  Columns declared in mappings but absent from df are skipped with a warning
   *  so a schema drift in one source does not abort the entire pipeline run. */
  def apply(df: DataFrame, mappings: List[AttributeMapping]): DataFrame = {
    val selectExprs = mappings.flatMap { mapping =>
      if (df.columns.contains(mapping.sourceName)) {
        val expr =
          if (mapping.mask)
            sha2(col(mapping.sourceName).cast(StringType), 256).as(mapping.targetName)
          else
            col(mapping.sourceName).as(mapping.targetName)
        Some(expr)
      } else {
        println(s"    Warning: column '${mapping.sourceName}' not found in DataFrame — skipped")
        None
      }
    }

    df.select(selectExprs: _*)
  }

  /** Returns the target name for a given source column, or the source name itself
   *  when no mapping is found (pass-through behaviour). */
  def getMappedName(sourceCol: String, mappings: List[AttributeMapping]): String =
    mappings.find(_.sourceName == sourceCol).map(_.targetName).getOrElse(sourceCol)
}
