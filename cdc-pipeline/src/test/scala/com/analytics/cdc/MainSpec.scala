package com.analytics.cdc

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MainSpec extends AnyWordSpec with Matchers {
  "Main.parseArgs" should {
    "parse --key=value pairs into a map" in {
      val result = Main.parseArgs(Array("--config=config.json", "--snap-date=2026-07-09"))
      result shouldBe Map("config" -> "config.json", "snap-date" -> "2026-07-09")
    }

    "parse a boolean-style flag with an explicit value" in {
      val result = Main.parseArgs(Array("--force=true"))
      result shouldBe Map("force" -> "true")
    }

    "ignore args without '=' or without the '--' prefix" in {
      val result = Main.parseArgs(Array("bogus", "--no-equals", "--config=x.json"))
      result shouldBe Map("config" -> "x.json")
    }

    "return an empty map for no args" in {
      Main.parseArgs(Array.empty) shouldBe Map.empty
    }
  }
}
