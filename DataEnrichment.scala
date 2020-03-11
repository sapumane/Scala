package ca.mcit.dataenrichment

import java.io._
import scala.io.Source

object DataEnrichment extends App {
  //*************************************************************************************
  //  *** Load data from CSV files into scala collections/structures
  //
  //  Create a List from trips.txt
  //*************************************************************************************

  val bufferedLocation = "/home/bd-user/Downloads/"
  val bufferedSourceTrips = Source.fromFile(bufferedLocation + "trips.txt")
  val tripList: List[Trip] = bufferedSourceTrips
    .getLines()
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Trip(n(0).toInt, n(1), n(2), n(3), n(4).toInt, n(5).toInt, n(6).toInt,
      if (n(7).isEmpty) None else Some(n(7)),
      if (n(8).isEmpty) None else Some(n(8))))
  bufferedSourceTrips.close

  // Create a List from routes.txt.  Filter for subway services only (route_type =1)

  val bufferedSourceRoute = Source.fromFile(bufferedLocation + "routes.txt")
  val routeList: List[Route] = bufferedSourceRoute
    .getLines()
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Route(n(0).toInt, n(1), n(2), n(3), n(4).toInt, n(5), n(6), n(7)))
    .filter(_.routeType == 1)
  bufferedSourceRoute.close

  // Create a List from calendar.txt.  Filter for Monday services only

  val bufferedSourceCalendar = Source.fromFile(bufferedLocation + "calendar.txt")
  val calendarList: List[Calendar] = bufferedSourceCalendar
    .getLines()
    .toList
    .tail
    .map(_.split(",", -1))
    .map(n => Calendar(n(0), n(1).toInt, n(2).toInt, n(3).toInt, n(4).toInt, n(5).toInt, n(6).toInt, n(7).toInt, n(8), n(9)))
    .filter(_.monday == 1)
  bufferedSourceCalendar.close


  //***************************************************************************
  //  Enrich the Trip data with Route and Calender info
  //
  //  Join Trips and Routes on the route_ID  using a Map
  //***************************************************************************

  val routeMap: RouteLookup = new RouteLookup(routeList)
  val routeTrips: List[RouteTrip] =
    tripList.map(line => RouteTrip(line, routeMap.lookup(line.routeId)))
      .filter(_.route != null)

  //
  //  Join routeTrips to Calender using a NestedLoopJoin
  //

  val enrichedTrips: List[JoinOutput] =
    new GenericNestedLoopJoin[RouteTrip, Calendar]((i, j) => i.trip.serviceId == j.serviceId)
      .join(routeTrips, calendarList)


  //***************************************************************************
  //  Now, in enrichedTrips we have a bit of a messy data structure :
  //  JoinOutput(RouteTrip(Trip(...),Route(...)),Calendar(...))
  //
  //  So we need to un-bundle it into the output format we need
  //    (Note, strictly speaking, because the assignment only asked for the Trip
  //           details to be supplied, we did not need to create such a complex
  //           structure containing all the Route and Calendar info, but as I may
  //           use this code in future, I left it like that,  to keep it more "generic"
  //*************************************************************************************

  val outDataLines: List[String] =
    enrichedTrips
      .map(n =>
        EnrichedTrip.formatOutput(n.left.asInstanceOf[RouteTrip].trip,
          n.left.asInstanceOf[RouteTrip].route,
          n.right.asInstanceOf[Calendar]))

  //  Create the output file

  val outFile = new File(bufferedLocation + "SubwayTrips.csv")
  val bw = new BufferedWriter(new FileWriter(outFile))

  val l1 = List("route_id", "service_id", "trip_id", "trip_headsign",
    "direction_id", "shape_id", "wheelchair_accessible",
    "note_fr", "note_en", "route_long_name")
  for (line <- l1) {
    bw.write(line + ",")
  }

  for (line <- outDataLines) {
    bw.newLine()
    bw.write(line)
  }

  bw.close()


  //  display output to console


  printf("%-5s| %-20s| %-35s| %-50s| %-5s| %-5s| %-7s| %-7s| %-20s| %-20s|",
    "Route", "Service", "Trip ID", "Trip Head Sign", "Dir", "Shape", "WChair", "Subway", "Note French", "Note Eng")
  println()

  for (line <- outDataLines) {
    val b: Array[String] = line.split(",", -1)
    printf("%-5s| %-20s| %-35s| %-50s| %-5s| %-5s| %-7s| %-7s| %-20s| %-20s|",
      b(0), b(1), b(2), b(3), b(4), b(5), b(6), b(9), b(7), b(8))
    println()
  }
  println()
  println(s"Data enrichment complete.  ${outDataLines.size} records written to file SubwayTrips.csv")

}