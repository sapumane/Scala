package ca.mcit.dataenrichment

case class Calendar(
                     serviceId: String,
                     monday : Int,
                     tuesday: Int,
                     wednesday: Int,
                     thursday: Int,
                     friday: Int,
                     saturday: Int,
                     sunday: Int,
                     startDate : String,
                     endDate :String
                   )