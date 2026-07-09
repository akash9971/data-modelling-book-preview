package com.analytics.cdc.engine

import com.analytics.cdc.SparkTestBase
import com.analytics.cdc.model.{CdcColumns, CdcOp, RecordState}
import com.analytics.cdc.util.Util
import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HashBasedCdcStrategySpec extends AnyWordSpec with Matchers with SparkTestBase {

  private def hashed(rows: Seq[(Int, String)]): DataFrame = {
    val session = spark
    import session.implicits._
    Util.addContentHash(rows.toDF("id", "value"))
  }

  private def activeEmoFrom(today: DataFrame, version: Int = 1): DataFrame =
    today
      .withColumn(CdcColumns.Version, org.apache.spark.sql.functions.lit(version))
      .withColumn(CdcColumns.OpType, org.apache.spark.sql.functions.lit(RecordState.Active.label))

  "HashBasedCdcStrategy.diff" should {

    "treat every row as INSERT when activeEmo is None (bootstrap)" in {
      val today = hashed(Seq((1, "a"), (2, "b")))

      val result = HashBasedCdcStrategy.diff(today, None, List("id"))

      result.newVersionRows.count() shouldBe 2
      result.expiredKeys.count() shouldBe 0
      result.deletedKeys.count() shouldBe 0

      val rows = result.newVersionRows.collect()
      rows.foreach { row =>
        row.getAs[String](CdcColumns.OpName) shouldBe CdcOp.Insert.label
        row.getAs[String](CdcColumns.OpType) shouldBe RecordState.Active.label
        row.getAs[Int](CdcColumns.Version) shouldBe 1
        row.getAs[String](CdcColumns.PrevHashKey) shouldBe null
      }
    }

    "emit INSERT for a PK present today but absent from active emo" in {
      val active = hashed(Seq((1, "a")))
      val active2 = activeEmoFrom(active)
      val today = hashed(Seq((1, "a"), (2, "b")))

      val result = HashBasedCdcStrategy.diff(today, Some(active2), List("id"))

      result.newVersionRows.count() shouldBe 1
      val row = result.newVersionRows.collect().head
      row.getAs[Int]("id") shouldBe 2
      row.getAs[String](CdcColumns.OpName) shouldBe CdcOp.Insert.label
      row.getAs[Int](CdcColumns.Version) shouldBe 1
    }

    "emit UPDATE with incremented version and prev_hash_key when content changes" in {
      val active = activeEmoFrom(hashed(Seq((1, "a"))), version = 3)
      val today = hashed(Seq((1, "a-changed")))

      val result = HashBasedCdcStrategy.diff(today, Some(active), List("id"))

      result.newVersionRows.count() shouldBe 1
      val row = result.newVersionRows.collect().head
      row.getAs[String](CdcColumns.OpName) shouldBe CdcOp.Update.label
      row.getAs[Int](CdcColumns.Version) shouldBe 4
      row.getAs[String](CdcColumns.PrevHashKey) should not be null

      result.expiredKeys.count() shouldBe 1
      result.expiredKeys.collect().head.getAs[Int]("id") shouldBe 1
    }

    "emit no rows when content is unchanged" in {
      val active = activeEmoFrom(hashed(Seq((1, "a"))))
      val today = hashed(Seq((1, "a")))

      val result = HashBasedCdcStrategy.diff(today, Some(active), List("id"))

      result.newVersionRows.count() shouldBe 0
      result.expiredKeys.count() shouldBe 0
      result.deletedKeys.count() shouldBe 0
    }

    "emit deletedKeys for a PK present in active emo but absent today" in {
      val active = activeEmoFrom(hashed(Seq((1, "a"), (2, "b"))))
      val today = hashed(Seq((1, "a")))

      val result = HashBasedCdcStrategy.diff(today, Some(active), List("id"))

      result.newVersionRows.count() shouldBe 0
      result.deletedKeys.count() shouldBe 1
      result.deletedKeys.collect().head.getAs[Int]("id") shouldBe 2
    }

    "handle insert, update, unchanged, and delete together in one call" in {
      val active = activeEmoFrom(hashed(Seq((1, "unchanged"), (2, "will-change"), (3, "will-delete"))))
      val today  = hashed(Seq((1, "unchanged"), (2, "changed"), (4, "new")))

      val result = HashBasedCdcStrategy.diff(today, Some(active), List("id"))

      result.newVersionRows.count() shouldBe 2 // id=2 UPDATE, id=4 INSERT
      result.expiredKeys.count() shouldBe 1     // id=2
      result.deletedKeys.count() shouldBe 1     // id=3
    }
  }
}
