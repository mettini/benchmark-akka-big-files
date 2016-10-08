import akka.NotUsed

object BenchmarkAkkaStream {
  import akka.actor.ActorSystem
  import akka.stream._
  import akka.stream.scaladsl._
  import scala.util._
  import GraphDSL.Implicits._
  import scalikejdbc._
  import scalikejdbc.config._
  import java.util.Date
  import scala.concurrent.{Future, ExecutionContext}
  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val system = ActorSystem("Sys")
  implicit val materializer = ActorMaterializer()
  implicit val session = AutoSession

  def main(args: Array[String]): Unit = {
    val started = new Date()
    DBs.setupAll()
    val src = Source.fromIterator(() => io.Source.fromFile("/tmp/2008.csv", "utf-8").getLines())
    val parse = Flow[String].mapAsyncUnordered(2)(l => Future.successful(l.split(",").map(_.trim)))
    val csvToFlightEvent = Flow[Array[String]].mapAsyncUnordered(2)(a => Future.successful(FlightEvent.stringArrayToFlightEvent(a)))
    val filter = Flow[FlightEvent].filter(_.arrDelayMins > 0)
    val persist = Flow[FlightEvent].mapAsyncUnordered(50)(FlightEvent.persistEvent _)
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
}
