package com.analytics.cdc.model

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class CdcOpSpec extends AnyWordSpec with Matchers {
  "CdcOp" should {
    "expose the correct label for each case" in {
      CdcOp.Insert.label shouldBe "INSERT"
      CdcOp.Update.label shouldBe "UPDATE"
      CdcOp.Delete.label shouldBe "DELETE"
    }

    "round-trip via fromLabel" in {
      CdcOp.fromLabel("INSERT") shouldBe CdcOp.Insert
      CdcOp.fromLabel("UPDATE") shouldBe CdcOp.Update
      CdcOp.fromLabel("DELETE") shouldBe CdcOp.Delete
    }

    "throw IllegalArgumentException on an unknown label" in {
      an[IllegalArgumentException] should be thrownBy CdcOp.fromLabel("BOGUS")
    }
  }
}
