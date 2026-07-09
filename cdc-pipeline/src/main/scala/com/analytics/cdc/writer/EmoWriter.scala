package com.analytics.cdc.writer

import com.analytics.cdc.format.DataFormat
import com.analytics.cdc.model.{CdcColumns, CdcOp, RecordState}
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}

object EmoWriter {

  def appendNew(spark: SparkSession, format: DataFormat, emoPath: String, rows: DataFrame): Unit = {
    if (rows.isEmpty) return
    format.write(spark, rows, emoPath, "append")
  }

  def applyLifecycleUpdate(
    spark:       SparkSession,
    emoPath:     String,
    expiredKeys: DataFrame,
    deletedKeys: DataFrame,
    primaryKeys: List[String]
  ): Unit = {
    val expiredCond = keysCondition(primaryKeys, expiredKeys.collect())
    val deletedCond = keysCondition(primaryKeys, deletedKeys.collect())

    val matchCond = List(expiredCond, deletedCond).flatten.reduceOption(_ || _)
    matchCond.foreach { anyMatch =>
      val target = DeltaTable.forPath(spark, emoPath)
      val activeCond = col(CdcColumns.OpType) === RecordState.Active.label

      val opTypeExpr = deletedCond
        .map(d => when(d, lit(RecordState.Deleted.label)).otherwise(lit(RecordState.Expired.label)))
        .getOrElse(lit(RecordState.Expired.label))

      val opNameExpr = deletedCond
        .map(d => when(d, lit(CdcOp.Delete.label)).otherwise(col(CdcColumns.OpName)))
        .getOrElse(col(CdcColumns.OpName))

      target.update(
        activeCond && anyMatch,
        Map(
          CdcColumns.OpType -> opTypeExpr,
          CdcColumns.OpName -> opNameExpr
        )
      )
    }
  }

  private def keysCondition(primaryKeys: List[String], rows: Array[Row]): Option[Column] =
    if (rows.isEmpty) None
    else Some(
      rows.map { row =>
        primaryKeys.map(pk => col(pk) === lit(row.getAs[Any](pk))).reduce(_ && _)
      }.reduce(_ || _)
    )
}
