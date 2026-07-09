package com.analytics.cdc.config

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import play.api.libs.json.Json

class CdcConfigParserSpec extends AnyWordSpec with Matchers {

  val fullJson = Json.parse("""
    {
      "cdc_strategy": "hash_based",
      "tables": [
        {
          "model_name": "session_tracker",
          "lmo_path": "abfss://lmo/tracking/session_tracker",
          "emo_path": "abfss://emo/tracking/session_tracker",
          "primary_keys": ["session_id"],
          "filter": "status != 'TEST'",
          "mappings": [
            { "source_name": "session_start_time", "target_name": "session_start_ts", "mask": false },
            { "source_name": "user_id", "target_name": "user_id", "mask": true }
          ],
          "derivations": [
            { "target_name": "session_duration_mins", "expression": "(unix_timestamp(session_end_ts) - unix_timestamp(session_start_ts))/60" }
          ]
        }
      ]
    }
  """)

  val minimalJson = Json.parse("""
    {
      "tables": [
        {
          "model_name": "device_reference",
          "lmo_path": "abfss://lmo/reference/device_master",
          "emo_path": "abfss://emo/reference/device_master",
          "primary_keys": ["device_code"]
        }
      ]
    }
  """)

  "CdcConfigParser.parseJson" should {
    "parse a fully populated table entry" in {
      val config = CdcConfigParser.parseJson(fullJson)
      config.cdcStrategy shouldBe "hash_based"
      config.tables should have size 1

      val table = config.tables.head
      table.modelName shouldBe "session_tracker"
      table.lmoPath shouldBe "abfss://lmo/tracking/session_tracker"
      table.emoPath shouldBe "abfss://emo/tracking/session_tracker"
      table.primaryKeys shouldBe List("session_id")
      table.filter shouldBe Some("status != 'TEST'")
      table.mappings shouldBe List(
        ColumnMapping("session_start_time", "session_start_ts", mask = false),
        ColumnMapping("user_id", "user_id", mask = true)
      )
      table.derivations shouldBe List(
        DerivationRule("session_duration_mins",
          "(unix_timestamp(session_end_ts) - unix_timestamp(session_start_ts))/60")
      )
    }

    "default cdc_strategy, filter, mappings, and derivations when absent" in {
      val config = CdcConfigParser.parseJson(minimalJson)
      config.cdcStrategy shouldBe "hash_based"

      val table = config.tables.head
      table.filter shouldBe None
      table.mappings shouldBe List.empty
      table.derivations shouldBe List.empty
      table.primaryKeys shouldBe List("device_code")
    }
  }

  "CdcConfigParser.parse" should {
    "read and parse a config file from disk" in {
      val tmpFile = java.nio.file.Files.createTempFile("cdc-config", ".json")
      java.nio.file.Files.write(tmpFile, minimalJson.toString.getBytes)

      val config = CdcConfigParser.parse(tmpFile.toString)
      config.tables.head.modelName shouldBe "device_reference"
    }
  }
}
