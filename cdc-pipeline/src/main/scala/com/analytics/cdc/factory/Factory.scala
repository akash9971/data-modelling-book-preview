package com.analytics.cdc.factory

import com.analytics.cdc.engine.{CdcStrategy, HashBasedCdcStrategy}
import com.analytics.cdc.format.{DataFormat, DeltaFormat, ParquetFormat}

object Factory {

  def format(name: String): DataFormat = name.toLowerCase match {
    case "parquet" => new ParquetFormat
    case "delta"   => new DeltaFormat
    case other     => throw new IllegalArgumentException(
      s"Unknown data format: '$other'. Supported: parquet, delta"
    )
  }

  def strategy(name: String): CdcStrategy = name.toLowerCase match {
    case "hash_based" => HashBasedCdcStrategy
    case other        => throw new IllegalArgumentException(
      s"Unknown CDC strategy: '$other'. Supported: hash_based"
    )
  }
}
