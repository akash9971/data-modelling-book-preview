package com.analytics.cdc.util

import com.analytics.cdc.SparkTestBase
import com.analytics.cdc.config.ColumnMapping
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MappingTransformerSpec extends AnyWordSpec with Matchers with SparkTestBase {

  "Util.applyMappings" should {
    "rename a mapped column" in {
      val session = spark
      import session.implicits._
      val df = Seq((1, "2026-01-01")).toDF("id", "start_time")

      val result = Util.applyMappings(df, List(ColumnMapping("start_time", "start_ts", mask = false)))

      result.columns should contain("start_ts")
      result.columns should not contain "start_time"
      result.select("start_ts").as[String].collect().head shouldBe "2026-01-01"
    }

    "SHA-256 mask a column when mask = true" in {
      val session = spark
      import session.implicits._
      val df = Seq((1, "a@example.com")).toDF("id", "email")

      val result = Util.applyMappings(df, List(ColumnMapping("email", "email", mask = true)))

      val masked = result.select("email").as[String].collect().head
      masked should not be "a@example.com"
      masked should have length 64
    }

    "pass through unmapped columns unchanged" in {
      val session = spark
      import session.implicits._
      val df = Seq((1, "a", "b")).toDF("id", "mapped", "untouched")

      val result = Util.applyMappings(df, List(ColumnMapping("mapped", "renamed", mask = false)))

      result.columns should contain("untouched")
      result.select("untouched").as[String].collect().head shouldBe "b"
    }

    "return the DataFrame unchanged when mappings is empty" in {
      val session = spark
      import session.implicits._
      val df = Seq((1, "a")).toDF("id", "val")

      val result = Util.applyMappings(df, List.empty)

      result.columns.toSet shouldBe df.columns.toSet
    }
  }
}
