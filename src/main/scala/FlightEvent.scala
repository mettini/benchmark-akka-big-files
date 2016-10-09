import scala.concurrent.{Future, ExecutionContext}
import scala.util._
import scalikejdbc._
import scalikejdbc.config._

import scala.concurrent.ExecutionContext.Implicits.global

case class FlightEvent(year: String, month: String, dayOfMonth: String, depTime: String, 
                       uniqueCarrier: String, flightNum: String, tailNum: String, 
                       origin: String, destination: String, arrDelayMins: Int) {
  def flightDate = s"${year}-${month}-${dayOfMonth} ${flightHour}"
  def flightHour = s"${if (depTime.substring(0, 2) == "24") "23" else depTime.substring(0, 2) }:${depTime.substring(2, 4)}:00"
}

object FlightEvent {

  def stringArrayToFlightEvent(cols: Array[String]) = FlightEvent(cols(0), cols(1), cols(2), 
    if (isAllDigits(cols(4))) "%04d".format(cols(4).toInt) else "", cols(8), cols(9), cols(10), cols(16), 
    cols(17), Try(cols(14).toInt).getOrElse(-1))

  def stringArrayToSeq(cols: Array[String]) = {
    val e = stringArrayToFlightEvent(cols)
    Seq[Any](e.uniqueCarrier, e.flightNum, e.tailNum, e.flightDate, e.origin, e.destination, e.arrDelayMins)
  }

  def isDelayed(cols: Array[String]): Boolean = (Try(cols(14).toInt).getOrElse(-1)) > 0

  def persistEvent(e: FlightEvent)(implicit session: DBSession): Future[FlightEvent] = Future {
    // sql" insert into flight_delays (carrier, flight_number, tail_number, flight_date, origin, destination, delay) values (${e.uniqueCarrier}, ${e.flightNum}, ${e.tailNum}, ${e.flightDate}, ${e.origin}, ${e.destination} ,${e.arrDelayMins})".update.apply()
    e
  }

  private def isAllDigits(x: String) = x forall Character.isDigit
  
}
