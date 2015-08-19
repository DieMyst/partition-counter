package ru.diemyst

import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.stream.io.{Framing, SynchronousFileSource}
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

/**
 * User: diemust
 * Date: 18.08.2015
 * Time: 21:33
 */

case class TopicsList(topics: List[String])
case class LastTimestamp(timestamp: String)
case class LastTimestampStats(sum: Long, min: Long, max: Long, average: Long)
case class LastRunPartitionList(partList: List[(String, String)])

case class FoldResult(min: Long, max: Long, sum: Long, count: Int) {
  def toStats: LastTimestampStats = LastTimestampStats(count, min, max, sum/count)
}

object Protocols extends DefaultJsonProtocol with SprayJsonSupport {
  implicit def topicListFormat = jsonFormat1(TopicsList.apply)
  implicit def lastTimestampFormat = jsonFormat1(LastTimestamp.apply)
  implicit def lastTimestampStatsFormat = jsonFormat4(LastTimestampStats.apply)
  implicit def lastRunPartitionListFormat = jsonFormat1(LastRunPartitionList.apply)
}

class HttpFrontend(dir: String)(implicit system: ActorSystem) {
  import Protocols._
  import akka.http.scaladsl.model.MediaTypes._

  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(60.seconds)

  val log = Logging(system, classOf[HttpFrontend])

  val host = "127.0.0.1"
  val port = 8080

  val route: Route = {
    get {
      mapResponseEntity(_.withContentType(`application/json`)) {
        pathPrefix("topic_list") {
          complete {
            val list = new File(dir).listFiles().filter(_.isDirectory).map(_.getName)
            TopicsList(list.toList)
          }
        } ~
          (pathPrefix("last_timestamp") & parameter('topicname)) { topicname =>
            complete {
              LastTimestamp(lastTimestamp(dir, topicname).getName)
            }
          } ~
          (pathPrefix("last_stats") & parameter('topicname)) { topicname =>

            complete {
              println("last_timestamp_stats")
              val last = lastTimestamp(dir, topicname)
              val res = SynchronousFileSource(new File(last.getAbsolutePath + File.separator + "offsets.csv"))
                .via(Framing.delimiter(ByteString("\r\n"), maximumFrameLength = 100, allowTruncation = true))
                .map(_.utf8String)
                .map(_.split(",")(1).toLong)
                .runWith(Sink.fold(FoldResult(Long.MaxValue, 0, 0, 0)){case (FoldResult(min, max, sum, count), num) =>
                  FoldResult(
                    if (min > num) num else min,
                    if (max < num) num else max,
                    sum + num,
                    count + 1
                  )
                })
              res.map(_.toStats)
            }
          } ~
          (pathPrefix("last_run_partition_list") & parameter('topicname)) { topicname =>

            complete {
              println("last_run_partition_list")
              val last = lastTimestamp(dir, topicname)
              val list = Source.fromFile(last.getAbsolutePath + File.separator + "offsets.csv", "UTF-8").getLines()
                .map{str =>
                  val arr = str.split(",")
                  (arr(0), arr(1))
                }
              LastRunPartitionList(list.toList)
            }
          }
      }
    }
  }

  def lastTimestamp(dir: String, topicname: String): File = {
    val file = new File(dir + File.separator + topicname + File.separator + "history")
    println(file.getAbsolutePath + " " + file.exists())
    file.listFiles().sortBy(_.getName).last
  }

  /*def g(in: akka.stream.scaladsl.Source[Long, Long]) = Flow() { implicit b =>
    import FlowGraph.Implicits._

    val out = Sink.head[Long]

    val bcast = b.add(Broadcast[Long](4))
    val merge = b.add(Merge[Long](4))

    val flowMax = Flow[Long].fold[Long](0)((x, y) => if (x > y) x else y)
    val flowMin = Flow[Long].fold[Long](0)((x, y) => if (x < y) x else y)
    val flowSum = Flow[Long].fold[Long](0)(_ + _)
    val flowCount = Flow[Long].fold[Long](0)((r,c) => r + 1)

    in ~> bcast ~> flowMax ~> merge ~> out
          bcast ~> flowMin   ~> merge
          bcast ~> flowSum   ~> merge
          bcast ~> flowCount ~> merge
  }*/


  val serverBinding = Http(system).bindAndHandle(interface = host, port = port, handler = route).onSuccess {
    case _ => log.info(s"akka-http server started on $host:$port")
  }
}

object Testing extends App {
  implicit val system = ActorSystem("test-system", ConfigFactory.parseString( """
                                                                                |akka {
                                                                                |  loggers = ["akka.event.Logging$DefaultLogger"]
                                                                                |  loglevel = "DEBUG"
                                                                                |}
                                                                              """.stripMargin))
  new HttpFrontend("C:\\Users\\diemust\\Documents\\GitHub\\partition-counter\\src\\test\\resources\\test_dir")
}
