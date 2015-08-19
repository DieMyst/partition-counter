package ru.diemyst

import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
 * User: diemust
 * Date: 18.08.2015
 * Time: 21:33
 */

case class TopicsList(topics: List[String])
case class LastTimestamp(timestamp: String)
case class LastTimestampStats(sum: Long, max: Long, min: Long, average: Long)
case class LastRunPartitionList(partList: List[(Int, Long)])

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
              val file = new File(dir + File.separator + topicname + File.separator + "history")
              println(file.getAbsolutePath + " " + file.exists())
              val timestamp = file.listFiles().map(_.getName).sorted.last
              LastTimestamp(timestamp)
            }
          } ~
          pathPrefix("last_timestamp_stats") {

            complete {
              println("last_timestamp_stats")

              StatusCodes.OK
            }
          } ~
          pathPrefix("last_run_partition_list") {

            complete {
              println("last_run_partition_list")

              StatusCodes.OK
            }
          }
      }
    }
  }


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
