package com.analytics.cdc.writer

import com.analytics.cdc.SparkTestBase
import com.analytics.cdc.factory.Factory
import com.analytics.cdc.model.{CdcColumns, CdcOp, RecordState}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class EmoWriterSpec extends AnyWordSpec with Matchers with SparkTestBase {
  val deltaFormat = Factory.format("delta")

  private def seedEmo(path: String): Unit = {
    val session = spark
    import session.implicits._
    val seed = Seq((1, "a", 1), (2, "b", 1), (3, "c", 1))
      .toDF("id", "value", CdcColumns.Version)
      .withColumn(CdcColumns.HashKey, org.apache.spark.sql.functions.lit("h"))
      .withColumn(CdcColumns.PrevHashKey, org.apache.spark.sql.functions.lit(null: String))
      .withColumn(CdcColumns.OpName, org.apache.spark.sql.functions.lit(CdcOp.Insert.label))
      .withColumn(CdcColumns.OpType, org.apache.spark.sql.functions.lit(RecordState.Active.label))
    deltaFormat.write(spark, seed, path, "overwrite")
  }

  "EmoWriter.appendNew" should {
    "create the emo table on first write (bootstrap)" in {
      val session = spark
      import session.implicits._
      val path = tempDir("emo-append-bootstrap")
      val rows = Seq((1, "a")).toDF("id", "value")

      EmoWriter.appendNew(spark, deltaFormat, path, rows)

      deltaFormat.exists(spark, path) shouldBe true
      deltaFormat.read(spark, path).count() shouldBe 1
    }

    "append rows to an existing emo table" in {
      val session = spark
      import session.implicits._
      val path = tempDir("emo-append-existing")
      seedEmo(path)
      val newRows = Seq((4, "d", 1))
        .toDF("id", "value", CdcColumns.Version)
        .withColumn(CdcColumns.HashKey, org.apache.spark.sql.functions.lit("h2"))
        .withColumn(CdcColumns.PrevHashKey, org.apache.spark.sql.functions.lit(null: String))
        .withColumn(CdcColumns.OpName, org.apache.spark.sql.functions.lit(CdcOp.Insert.label))
        .withColumn(CdcColumns.OpType, org.apache.spark.sql.functions.lit(RecordState.Active.label))

      EmoWriter.appendNew(spark, deltaFormat, path, newRows)

      deltaFormat.read(spark, path).count() shouldBe 4
    }

    "do nothing when rows is empty" in {
      val session = spark
      import session.implicits._
      val path = tempDir("emo-append-empty")
      seedEmo(path)
      val empty = Seq.empty[(Int, String)].toDF("id", "value")

      EmoWriter.appendNew(spark, deltaFormat, path, empty)

      deltaFormat.read(spark, path).count() shouldBe 3
    }
  }

  "EmoWriter.applyLifecycleUpdate" should {
    "flip matching rows to EXPIRED and keep their op_name" in {
      val session = spark
      import session.implicits._
      val path = tempDir("emo-lifecycle-expire")
      seedEmo(path)
      val expiredKeys = Seq((1)).toDF("id")
      val deletedKeys = Seq.empty[Int].toDF("id")

      EmoWriter.applyLifecycleUpdate(spark, path, expiredKeys, deletedKeys, List("id"))

      val result = deltaFormat.read(spark, path).where("id = 1").collect().head
      result.getAs[String](CdcColumns.OpType) shouldBe RecordState.Expired.label
      result.getAs[String](CdcColumns.OpName) shouldBe CdcOp.Insert.label

      deltaFormat.read(spark, path).where("id = 2").collect().head
        .getAs[String](CdcColumns.OpType) shouldBe RecordState.Active.label
    }

    "flip matching rows to DELETED and set op_name to DELETE" in {
      val session = spark
      import session.implicits._
      val path = tempDir("emo-lifecycle-delete")
      seedEmo(path)
      val expiredKeys = Seq.empty[Int].toDF("id")
      val deletedKeys = Seq((2)).toDF("id")

      EmoWriter.applyLifecycleUpdate(spark, path, expiredKeys, deletedKeys, List("id"))

      val result = deltaFormat.read(spark, path).where("id = 2").collect().head
      result.getAs[String](CdcColumns.OpType) shouldBe RecordState.Deleted.label
      result.getAs[String](CdcColumns.OpName) shouldBe CdcOp.Delete.label
    }

    "handle expire and delete together in one call" in {
      val session = spark
      import session.implicits._
      val path = tempDir("emo-lifecycle-both")
      seedEmo(path)
      val expiredKeys = Seq((1)).toDF("id")
      val deletedKeys = Seq((2)).toDF("id")

      EmoWriter.applyLifecycleUpdate(spark, path, expiredKeys, deletedKeys, List("id"))

      val all = deltaFormat.read(spark, path)
      all.where("id = 1").collect().head.getAs[String](CdcColumns.OpType) shouldBe RecordState.Expired.label
      all.where("id = 2").collect().head.getAs[String](CdcColumns.OpType) shouldBe RecordState.Deleted.label
      all.where("id = 3").collect().head.getAs[String](CdcColumns.OpType) shouldBe RecordState.Active.label
    }

    "do nothing when both key sets are empty, even if the table doesn't exist yet" in {
      val session = spark
      import session.implicits._
      val path = tempDir("emo-lifecycle-noop")
      val expiredKeys = Seq.empty[Int].toDF("id")
      val deletedKeys = Seq.empty[Int].toDF("id")

      noException should be thrownBy
        EmoWriter.applyLifecycleUpdate(spark, path, expiredKeys, deletedKeys, List("id"))
    }
  }
}
