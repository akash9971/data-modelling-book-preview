package com.analytics.cdc.format

import com.analytics.cdc.SparkTestBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DeltaFormatSpec extends AnyWordSpec with Matchers with SparkTestBase {
  val format = new DeltaFormat

  "DeltaFormat" should {
    "write then read back the same rows" in {
      val session = spark
      import session.implicits._
      val dir = tempDir("delta-format")
      val df = Seq((1, "a"), (2, "b")).toDF("id", "val")

      format.write(spark, df, dir, "append")
      val readBack = format.read(spark, dir)

      readBack.count() shouldBe 2
    }

    "report exists = false for a path that was never written" in {
      val dir = tempDir("delta-format-missing") + "/never-written"
      format.exists(spark, dir) shouldBe false
    }

    "report exists = true after a write" in {
      val session = spark
      import session.implicits._
      val dir = tempDir("delta-format-exists")
      format.write(spark, Seq((1, "a")).toDF("id", "val"), dir, "append")

      format.exists(spark, dir) shouldBe true
    }
  }
}
