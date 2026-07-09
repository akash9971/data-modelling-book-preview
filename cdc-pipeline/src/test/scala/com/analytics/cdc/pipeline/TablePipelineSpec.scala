package com.analytics.cdc.pipeline

import com.analytics.cdc.SparkTestBase
import com.analytics.cdc.config.TableConfig
import com.analytics.cdc.engine.HashBasedCdcStrategy
import com.analytics.cdc.factory.Factory
import com.analytics.cdc.model.{CdcColumns, CdcOp, RecordState}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class TablePipelineSpec extends AnyWordSpec with Matchers with SparkTestBase {
  val deltaFormat = Factory.format("delta")
  val pipeline    = new TablePipeline(HashBasedCdcStrategy)

  private def writeLmoSnap(lmoPath: String, snapDate: String, rows: Seq[(Int, String)]): Unit = {
    val session = spark
    import session.implicits._
    rows.toDF("id", "value").write.mode("overwrite").parquet(s"$lmoPath/$snapDate")
  }

  "TablePipeline.run" should {
    "bootstrap: first run makes every row an ACTIVE INSERT at version 1" in {
      val lmoPath = tempDir("pipeline-lmo-1")
      val emoPath = tempDir("pipeline-emo-1") + "/table"
      writeLmoSnap(lmoPath, "2026-07-07", Seq((1, "a"), (2, "b")))

      val table = TableConfig("t1", lmoPath, emoPath, List("id"), None, List.empty, List.empty)
      pipeline.run(spark, table, "2026-07-07")

      val emo = deltaFormat.read(spark, emoPath)
      emo.count() shouldBe 2
      emo.where(s"${CdcColumns.OpType} = '${RecordState.Active.label}'").count() shouldBe 2
      emo.collect().foreach(_.getAs[Int](CdcColumns.Version) shouldBe 1)
    }

    "skip a snap_date that was already processed unless force=true" in {
      val lmoPath = tempDir("pipeline-lmo-2")
      val emoPath = tempDir("pipeline-emo-2") + "/table"
      writeLmoSnap(lmoPath, "2026-07-07", Seq((1, "a")))
      val table = TableConfig("t2", lmoPath, emoPath, List("id"), None, List.empty, List.empty)

      pipeline.run(spark, table, "2026-07-07")
      pipeline.run(spark, table, "2026-07-07") // should be a no-op skip

      deltaFormat.read(spark, emoPath).count() shouldBe 1
    }

    "on a second snap_date: INSERT new rows, UPDATE changed rows, DELETE missing rows, leave unchanged rows alone" in {
      val lmoPath = tempDir("pipeline-lmo-3")
      val emoPath = tempDir("pipeline-emo-3") + "/table"
      writeLmoSnap(lmoPath, "2026-07-07", Seq((1, "unchanged"), (2, "will-change"), (3, "will-delete")))
      val table = TableConfig("t3", lmoPath, emoPath, List("id"), None, List.empty, List.empty)
      pipeline.run(spark, table, "2026-07-07")

      writeLmoSnap(lmoPath, "2026-07-08", Seq((1, "unchanged"), (2, "changed"), (4, "new")))
      pipeline.run(spark, table, "2026-07-08")

      val emo = deltaFormat.read(spark, emoPath)

      // id=1 unchanged: still the single ACTIVE version-1 row, untouched
      val id1Rows = emo.where("id = 1").collect()
      id1Rows should have length 1
      id1Rows.head.getAs[String](CdcColumns.OpType) shouldBe RecordState.Active.label
      id1Rows.head.getAs[Int](CdcColumns.Version) shouldBe 1

      // id=2 changed: old version EXPIRED, new version ACTIVE at version 2
      val id2Rows = emo.where("id = 2").collect().sortBy(_.getAs[Int](CdcColumns.Version))
      id2Rows should have length 2
      id2Rows(0).getAs[String](CdcColumns.OpType) shouldBe RecordState.Expired.label
      id2Rows(1).getAs[String](CdcColumns.OpType) shouldBe RecordState.Active.label
      id2Rows(1).getAs[Int](CdcColumns.Version) shouldBe 2
      id2Rows(1).getAs[String](CdcColumns.OpName) shouldBe CdcOp.Update.label

      // id=3 deleted: single row flipped to DELETED, op_name=DELETE
      val id3Rows = emo.where("id = 3").collect()
      id3Rows should have length 1
      id3Rows.head.getAs[String](CdcColumns.OpType) shouldBe RecordState.Deleted.label
      id3Rows.head.getAs[String](CdcColumns.OpName) shouldBe CdcOp.Delete.label

      // id=4 new: single ACTIVE INSERT row at version 1
      val id4Rows = emo.where("id = 4").collect()
      id4Rows should have length 1
      id4Rows.head.getAs[String](CdcColumns.OpName) shouldBe CdcOp.Insert.label
      id4Rows.head.getAs[Int](CdcColumns.Version) shouldBe 1
    }

    "apply filter, mappings, and derivations before diffing" in {
      val lmoPath = tempDir("pipeline-lmo-4")
      val emoPath = tempDir("pipeline-emo-4") + "/table"
      val session = spark
      import session.implicits._
      Seq((1, "keep", 10), (2, "drop", 99))
        .toDF("id", "status", "amount")
        .write.mode("overwrite").parquet(s"$lmoPath/2026-07-07")

      val table = TableConfig(
        modelName   = "t4",
        lmoPath     = lmoPath,
        emoPath     = emoPath,
        primaryKeys = List("id"),
        filter      = Some("status = 'keep'"),
        mappings    = List(com.analytics.cdc.config.ColumnMapping("amount", "amt", mask = false)),
        derivations = List(com.analytics.cdc.config.DerivationRule("amt_doubled", "amt * 2"))
      )

      pipeline.run(spark, table, "2026-07-07")

      val emo = deltaFormat.read(spark, emoPath)
      emo.count() shouldBe 1
      val row = emo.collect().head
      row.getAs[Int]("amt") shouldBe 10
      row.getAs[Int]("amt_doubled") shouldBe 20
    }
  }
}
