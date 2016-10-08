import akka.NotUsed
import scala.concurrent.{Future, ExecutionContext}

object BenchmarkAkkaStream {
  import akka.actor.ActorSystem
  import akka.stream._
  import akka.stream.scaladsl._
  import scala.util._
  import GraphDSL.Implicits._
  import scalikejdbc._
  import scalikejdbc.config._
  import java.util.Date

  import java.util.concurrent.Executors
  import scala.concurrent._

  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newFixedThreadPool(100)
    def execute(runnable: Runnable) { threadPool.submit(runnable) }
    def reportFailure(t: Throwable) {}
  }
  
  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()
  implicit val session = AutoSession

  def main(args: Array[String]): Unit = {
    val started = new Date()
    DBs.setupAll()
    val src = Source.fromIterator(() => io.Source.fromFile("/tmp/2008.csv", "utf-8").getLines())
    val parse = Flow[String].mapAsyncUnordered(2)(l => Future.successful(l.split(",").map(_.trim)))
    val csvToFlightEvent = Flow[Array[String]].mapAsyncUnordered(2)(a => Future.successful(stringArrayToFlightEvent(a)))
    val filter = Flow[FlightEvent].filter(_.arrDelayMins > 0)
    val persist = Flow[FlightEvent].mapAsyncUnordered(50)(persistEvent _)
    val count = Flow[FlightEvent].mapAsyncUnordered(2)(_ => Future.successful(1))
    val sum = Sink.fold[Int, Int](0)(_ + _)

    src.via(parse).via(csvToFlightEvent).via(filter).via(persist).via(count).runWith(sum).onComplete {
      case Success(f) =>
        val end = new Date()
        val diff = end.getTime() - started.getTime()
        println(s"Flights persisted: $f - Started: $started - Finished: ${end} - Duration: ${diff / (60 * 1000) % 60}min, ${diff / 1000 % 60}sec")
      case Failure(e) => println(s"Error procesing csv: $e")
      DBs.closeAll()
    }
    system.awaitTermination()
  }
  
  private def stringArrayToFlightEvent(cols: Array[String]) = new FlightEvent(cols(0), cols(1), cols(2), 
    if (isAllDigits(cols(4))) "%04d".format(cols(4).toInt) else "", cols(8), cols(9), cols(10), cols(16), 
    cols(17), Try(cols(14).toInt).getOrElse(-1))

  private def persistEvent(e: FlightEvent)(implicit ec: ExecutionContext): Future[FlightEvent] = Future {
    sql" insert into flight_delays (carrier, flight_number, tail_number, flight_date, origin, destination, delay) values (${e.uniqueCarrier}, ${e.flightNum}, ${e.tailNum}, ${flightDate(e)}, ${e.origin}, ${e.destination} ,${e.arrDelayMins})".update.apply()
    e
  }

  private def isAllDigits(x: String) = x forall Character.isDigit
  private def flightDate(e: FlightEvent) = s"${e.year}-${e.month}-${e.dayOfMonth} ${flightHour(e)}"
  private def flightHour(e: FlightEvent) = s"${if (e.depTime.substring(0, 2) == "24") "23" else e.depTime.substring(0, 2) }:${e.depTime.substring(2, 4)}:00"
}

case class FlightEvent(year: String, month: String, dayOfMonth: String, depTime: String, uniqueCarrier: String, 
                       flightNum: String, tailNum: String, origin: String, destination: String, arrDelayMins: Int)
