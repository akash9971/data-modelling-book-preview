package com.analytics.cdc.model

object CdcColumns {
  val HashKey     = "__hash_key"
  val PrevHashKey = "__prev_hash_key"
  val Version     = "__version"
  val OpName      = "op_name"
  val OpType      = "op_type"
  val SnapDate    = "snap_date"
  val EffectiveTs = "effective_ts"
}

sealed trait CdcOp { def label: String }

object CdcOp {
  case object Insert extends CdcOp { val label = "INSERT" }
  case object Update extends CdcOp { val label = "UPDATE" }
  case object Delete extends CdcOp { val label = "DELETE" }

  def fromLabel(label: String): CdcOp = label match {
    case "INSERT" => Insert
    case "UPDATE" => Update
    case "DELETE" => Delete
    case other    => throw new IllegalArgumentException(s"Unknown CdcOp label: '$other'")
  }
}

sealed trait RecordState { def label: String }

object RecordState {
  case object Active  extends RecordState { val label = "ACTIVE"  }
  case object Expired extends RecordState { val label = "EXPIRED" }
  case object Deleted extends RecordState { val label = "DELETED" }

  def fromLabel(label: String): RecordState = label match {
    case "ACTIVE"  => Active
    case "EXPIRED" => Expired
    case "DELETED" => Deleted
    case other     => throw new IllegalArgumentException(s"Unknown RecordState label: '$other'")
  }
}
