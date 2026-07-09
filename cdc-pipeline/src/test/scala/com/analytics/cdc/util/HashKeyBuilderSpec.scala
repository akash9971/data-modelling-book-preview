package com.analytics.cdc.util

import com.analytics.cdc.SparkTestBase
import com.analytics.cdc.model.CdcColumns
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HashKeyBuilderSpec extends AnyWordSpec with Matchers with SparkTestBase {

  "Util.addContentHash" should {
    "add a __hash_key column" in {
      val session = spark
      import session.implicits._
      val df = Seq((1, "a")).toDF("id", "val")

      val hashed = Util.addContentHash(df)

      hashed.columns should contain(CdcColumns.HashKey)
    }

    "produce identical hashes for identical content computed separately" in {
      val session = spark
      import session.implicits._
      val df1 = Seq((1, "a")).toDF("id", "val")
      val df2 = Seq((1, "a")).toDF("id", "val")

      val hash1 = Util.addContentHash(df1).select(CdcColumns.HashKey).as[String].collect().head
      val hash2 = Util.addContentHash(df2).select(CdcColumns.HashKey).as[String].collect().head

      hash1 shouldBe hash2
    }

    "produce a different hash when a field value changes" in {
      val session = spark
      import session.implicits._
      val original = Seq((1, "a")).toDF("id", "val")
      val changed  = Seq((1, "b")).toDF("id", "val")

      val hash1 = Util.addContentHash(original).select(CdcColumns.HashKey).as[String].collect().head
      val hash2 = Util.addContentHash(changed).select(CdcColumns.HashKey).as[String].collect().head

      hash1 should not be hash2
    }

    "throw on a DataFrame with no columns" in {
      val emptySchemaDf = spark.emptyDataFrame
      an[IllegalArgumentException] should be thrownBy Util.addContentHash(emptySchemaDf)
    }
  }
}
