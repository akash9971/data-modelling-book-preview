package com.analytics.cdc.util

import com.analytics.cdc.SparkTestBase
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SparkJobGroupSpec extends AnyWordSpec with Matchers with SparkTestBase {

  "Util.withJobGroup" should {
    "set the job group id for the duration of the block" in {
      var observedGroupId: String = null

      Util.withJobGroup(spark, "my-group", "my description") {
        observedGroupId = spark.sparkContext.getLocalProperty("spark.jobGroup.id")
      }

      observedGroupId shouldBe "my-group"
    }

    "clear the job group after the block completes" in {
      Util.withJobGroup(spark, "my-group", "my description") {
        ()
      }

      spark.sparkContext.getLocalProperty("spark.jobGroup.id") shouldBe null
    }

    "clear the job group even when the block throws" in {
      an[RuntimeException] should be thrownBy {
        Util.withJobGroup(spark, "my-group", "my description") {
          throw new RuntimeException("boom")
        }
      }

      spark.sparkContext.getLocalProperty("spark.jobGroup.id") shouldBe null
    }

    "return the block's value" in {
      val result = Util.withJobGroup(spark, "my-group", "my description") { 1 + 1 }
      result shouldBe 2
    }
  }
}
