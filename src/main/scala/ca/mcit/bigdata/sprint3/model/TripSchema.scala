package ca.mcit.bigdata.sprint3.model

case class TripSchema(start_date: String, start_station_code: Int, end_date: String, end_station_code: Int, duration_sec: Int, is_member: Int)
object TripSchema {
  def apply(csv: String): TripSchema = {
    val fields = csv.split(",", -1)
    TripSchema(fields(0),fields(1).toInt,fields(2),fields(3).toInt,fields(4).toInt,fields(5).toInt)
  }
}