package ca.mcit.dataenrichment

case class EnrichedTrip(trip: Trip, route: Route, calendar: Calendar)

object EnrichedTrip {

  def formatOutput(trip: Trip, route: Route, calendar: Calendar): String = {
      trip.routeId + "," +
      trip.serviceId + "," +
      trip.tripId + "," +
      trip.tripHeadSign + "," +
      trip.directionId + "," +
      trip.shapeId + "," +
      trip.wheelchairAccessible + "," +
      trip.noteFr.getOrElse("") + "," +
      trip.noteEn.getOrElse("") + "," +
      route.routeLongName
  }

}
