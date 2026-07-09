package com.analytics.cdc.factory

import com.analytics.cdc.engine.HashBasedCdcStrategy
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CdcStrategyFactorySpec extends AnyWordSpec with Matchers {
  "Factory.strategy" should {
    "return HashBasedCdcStrategy for 'hash_based'" in {
      Factory.strategy("hash_based") shouldBe HashBasedCdcStrategy
    }

    "be case-insensitive" in {
      Factory.strategy("HASH_BASED") shouldBe HashBasedCdcStrategy
    }

    "throw IllegalArgumentException for an unknown strategy" in {
      an[IllegalArgumentException] should be thrownBy Factory.strategy("column_diff")
    }
  }
}
