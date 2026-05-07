package com.analytics.util

import com.analytics.models.Model

/** Resolves model execution order based on declared lineage dependencies. */
object LineageResolver {

  /**
   * Topological sort — ensures each model runs only after all its dependencies
   * have been processed.  The DFS visits each model's lineage list recursively
   * before appending the model itself, naturally producing a valid order.
   *
   * Example order given the session-tracking models:
   *   message_detail_type → user_profile → device_reference
   *   → event_tracker → session_tracker → user_session_summary
   */
  def resolve(models: List[Model]): List[Model] = {
    val modelMap = models.map(m => m.name -> m).toMap
    val visited  = scala.collection.mutable.Set[String]()
    val result   = scala.collection.mutable.ListBuffer[Model]()

    def visit(name: String): Unit = {
      if (!visited.contains(name) && modelMap.contains(name)) {
        visited += name
        modelMap(name).lineage.foreach(visit)
        result += modelMap(name)
      }
    }

    models.foreach(m => visit(m.name))
    result.toList
  }
}
