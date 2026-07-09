package com.analytics.cdc.factory

import com.analytics.cdc.format.{DeltaFormat, ParquetFormat}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class FormatFactorySpec extends AnyWordSpec with Matchers {
  "Factory.format" should {
    "return a ParquetFormat for 'parquet'" in {
      Factory.format("parquet") shouldBe a[ParquetFormat]
    }

    "return a DeltaFormat for 'delta'" in {
      Factory.format("delta") shouldBe a[DeltaFormat]
    }

    "be case-insensitive" in {
      Factory.format("PARQUET") shouldBe a[ParquetFormat]
      Factory.format("Delta") shouldBe a[DeltaFormat]
    }

    "throw IllegalArgumentException for an unknown format" in {
      an[IllegalArgumentException] should be thrownBy Factory.format("avro")
    }
  }
}
