package com.analytics.factory

import com.analytics.format._
import com.analytics.writer._

/**
 * FACTORY PATTERN
 *
 * Central wiring: JSON config string → correct object instance.
 * All creation logic lives here; pipelines and StageManager never call new directly.
 *
 * To add a new model type or storage format:
 *   1. Create the class (e.g. SCDType3Writer, AvroFormat)
 *   2. Add one case to the matching factory
 *   3. Done — no pipeline code changes needed
 */

// =============================================================================
// WRITER FACTORY: model type string → ModelWriter
// =============================================================================

object WriterFactory {

  def create(modelType: String): ModelWriter = modelType.toLowerCase match {
    case "flat"     => new FlatWriter
    case "scdtype1" => new SCDType1Writer
    case "scdtype2" => new SCDType2Writer
    case other      => throw new IllegalArgumentException(
      s"Unknown model type: '$other'. Supported: flat, scdtype1, scdtype2"
    )
  }
}

// =============================================================================
// FORMAT FACTORY: format string → DataFormat
// =============================================================================

object FormatFactory {

  def create(format: String): DataFormat = format.toLowerCase match {
    case "delta"   => new DeltaFormat
    case "parquet" => new ParquetFormat
    case "iceberg" => new IcebergFormat
    case other     => throw new IllegalArgumentException(
      s"Unknown data format: '$other'. Supported: delta, parquet, iceberg"
    )
  }
}
