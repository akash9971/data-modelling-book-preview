package com.analytics.cdc.model

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class RecordStateSpec extends AnyWordSpec with Matchers {
  "RecordState" should {
    "expose the correct label for each case" in {
      RecordState.Active.label shouldBe "ACTIVE"
      RecordState.Expired.label shouldBe "EXPIRED"
      RecordState.Deleted.label shouldBe "DELETED"
    }

    "round-trip via fromLabel" in {
      RecordState.fromLabel("ACTIVE") shouldBe RecordState.Active
      RecordState.fromLabel("EXPIRED") shouldBe RecordState.Expired
      RecordState.fromLabel("DELETED") shouldBe RecordState.Deleted
    }

    "throw IllegalArgumentException on an unknown label" in {
      an[IllegalArgumentException] should be thrownBy RecordState.fromLabel("BOGUS")
    }
  }
}
