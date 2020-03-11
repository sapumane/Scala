package ca.mcit.dataenrichment

class RouteLookup(routes:  List[Route]) {
  private val lookupTable: Map[Int, Route] =
    routes.map(route => route.routeId -> route).toMap

  def lookup(routeId: Int): Route =
    lookupTable.getOrElse(routeId, null)

}