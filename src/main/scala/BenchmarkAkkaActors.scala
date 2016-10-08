import scala.concurrent.{Future, ExecutionContext}

object BenchmarkAkkaActors {
  import akka.actor.ActorSystem
  import scala.util._
  import scalikejdbc._
  import scalikejdbc.config._
  import java.util.Date

  implicit val system = ActorSystem("Sys")
  implicit val session = AutoSession

  def main(args: Array[String]): Unit = {
    val started = new Date()
    DBs.setupAll()

    // TODO: do the magic

    val end = new Date()
    val diff = end.getTime() - started.getTime()
    println(s"Flights persisted: 0 - Started: $started - Finished: ${end} - Duration: ${diff / (60 * 1000) % 60}min, ${diff / 1000 % 60}sec")
    DBs.closeAll()
    
    system.awaitTermination()
  }
}
