import scala.concurrent.{Future, ExecutionContext}

object BenchmarkScala {
  import scala.util._
  import scalikejdbc._
  import scalikejdbc.config._
  import java.util.Date
  import FlightEvent._
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val session = AutoSession

  def main(args: Array[String]): Unit = {
    val started = new Date()
    DBs.setupAll()
    
    val src = io.Source.fromFile("/tmp/2008.csv")
    val flights = src.getLines.map(_.split(",").map(_.trim)).filter(isDelayed _).map(stringArrayToSeq _)
    flights.grouped(10000).foreach(persistBulkEvents _)
    
    val end = new Date()
    val diff = end.getTime() - started.getTime()
    println(s"Started: $started - Finished: ${end}")
    println(s"Total duration: ${diff / (60 * 1000) % 60}min, ${diff / 1000 % 60}sec")
    DBs.closeAll()  
  }

  private def persistBulkEvents(e: Seq[Seq[Any]])(implicit session: DBSession) = {
    sql" insert into flight_delays (carrier, flight_number, tail_number, flight_date, origin, destination, delay) values (?, ?, ?, ?, ?, ?, ?)".batch(e: _*).apply()
  }

}
