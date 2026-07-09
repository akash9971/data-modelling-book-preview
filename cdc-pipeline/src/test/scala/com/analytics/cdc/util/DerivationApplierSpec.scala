package com.analytics.cdc.util

import com.analytics.cdc.SparkTestBase
import com.analytics.cdc.config.DerivationRule
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class DerivationApplierSpec extends AnyWordSpec with Matchers with SparkTestBase {

  "Util.applyDerivations" should {
    "add a computed column from a SQL expression" in {
      val session = spark
      import session.implicits._
      val df = Seq((10, 4)).toDF("a", "b")

      val result = Util.applyDerivations(df, List(DerivationRule("total", "a + b")))

      result.select("total").as[Int].collect().head shouldBe 14
    }

    "chain multiple derivations, later ones referencing earlier ones" in {
      val session = spark
      import session.implicits._
      val df = Seq((10, 4)).toDF("a", "b")

      val result = Util.applyDerivations(df, List(
        DerivationRule("total", "a + b"),
        DerivationRule("doubled_total", "total * 2")
      ))

      result.select("doubled_total").as[Int].collect().head shouldBe 28
    }

    "return the DataFrame unchanged when derivations is empty" in {
      val session = spark
      import session.implicits._
      val df = Seq((1, "a")).toDF("id", "val")

      val result = Util.applyDerivations(df, List.empty)

      result.columns.toSet shouldBe df.columns.toSet
    }
  }
}
