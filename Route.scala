package ca.mcit.dataenrichment

case class Route(
                  routeId: Int,
                  agencyId: String,
                  routeShortName: String,
                  routeLongName: String,
                  routeType: Int,
                  routeUrl: String,
                  routeColor: String,
                  routeTextColor: String
                )