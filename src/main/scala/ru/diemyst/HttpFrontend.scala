package ru.diemyst

import java.io.File

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.stream.ActorMaterializer
import akka.stream.io.{Framing, SynchronousFileSource}
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}
import spray.json.DefaultJsonProtocol

import scala.concurrent.Future
import scala.concurrent.duration._

/**
 * User: diemust
 * Date: 18.08.2015
 * Time: 21:33
 */
case class TopicsList(topics: List[String])
case class LastTimestamp(timestamp: String)
case class LastTimestampStats(sum: Long, min: Long, max: Long, average: Long)
case class LastRunPartitionList(partList: List[(String, String)])
case class Error(error: String)

case class FoldResult(min: Long, max: Long, sum: Long, count: Int) {
  def toStats: LastTimestampStats = LastTimestampStats(count, min, max, sum/count)
}

object Protocols extends DefaultJsonProtocol with SprayJsonSupport {
  implicit def topicListFormat = jsonFormat1(TopicsList.apply)
  implicit def lastTimestampFormat = jsonFormat1(LastTimestamp.apply)
  implicit def lastTimestampStatsFormat = jsonFormat4(LastTimestampStats.apply)
  implicit def lastRunPartitionListFormat = jsonFormat1(LastRunPartitionList.apply)
  implicit def errorFormat = jsonFormat1(Error.apply)
}

class HttpFrontend(dir: String)(implicit system: ActorSystem) {
  import Protocols._
  import akka.http.scaladsl.model.MediaTypes._

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val materializer = ActorMaterializer()
  implicit val timeout = Timeout(60.seconds)

  assert(new File(dir).exists())

  val cacheActor = system.actorOf(CacheTimestampActor.props)

  val log = Logging(system, classOf[HttpFrontend])

  val host = "127.0.0.1"
  val port = 8080

  val framing = Framing.delimiter(ByteString("\r\n"), maximumFrameLength = 100, allowTruncation = true)

  val route: Route = {
    get {
      mapResponseEntity(_.withContentType(`application/json`)) {
        pathPrefix("topic_list") {
          complete {
            Future {
              Option(new File(dir).listFiles()).filter(_.nonEmpty) match {
                case Some(arr) => Right(TopicsList(arr.filter(_.isDirectory).map(_.getName).toList))
                case None => Left(Error("No topics"))
              }
            }
          }
        } ~
        parameter('topicname) { topicname =>
          path("last_timestamp") {
            complete {
              (cacheActor ? GetLast(dir, topicname))
                .map{
                  case SuccessStamp(stamp) => Right(LastTimestamp(stamp))
                  case ErrorMsg(msg) => Left(Error(msg))
                }
            }
          } ~
          path("last_stats") {
            complete {
              (cacheActor ? GetLast(dir, topicname)).map{
                    case SuccessStamp(stamp) => Right(getStats(getCsvName(dir, topicname, stamp)))
                    case ErrorMsg(msg) => Left(Error(msg))
                }
            }
          } ~
          path("last_run_partition_list") {
            complete {
              (cacheActor ? GetLast(dir, topicname)).map{
                case SuccessStamp(stamp) => Right(getList(getCsvName(dir, topicname, stamp)))
                case ErrorMsg(msg) => Left(Error(msg))
              }
            }
          }
        }
      }
    }
  }

  def getList(path: String): Future[LastRunPartitionList] = {
    SynchronousFileSource(new File(path))
      .via(framing)
      .map(_.utf8String)
      .map{str =>
      val arr = str.split(",")
      (arr(0), arr(1))
    }.runWith(Sink.fold(List.empty[(String, String)]){case (list, tuple) => list :+ tuple})
      .map(LastRunPartitionList)
  }

  def getStats(path: String): Future[LastTimestampStats] = {
    SynchronousFileSource(new File(path))
      .via(framing)
      .map(_.utf8String)
      .map(_.split(",")(1).toLong)
      .runWith(Sink.fold(FoldResult(Long.MaxValue, 0, 0, 0)){
      case (FoldResult(min, max, sum, count), num) =>
        FoldResult(
          if (min > num) num else min,
          if (max < num) num else max,
          //если подразумевается, что сумма может выйти за рамки Long, то можно взять бОльший тип или считать среднее
          //как x1/n + x2/n + ... , где n - число строк в файле
          sum + num,
          count + 1
        )
    }).map(_.toStats)
  }

  def getCsvName(dir: String, topicname: String, stamp: String) = dir + File.separator + topicname + File.separator + "history" + File.separator + stamp + File.separator + "offsets.csv"

  val serverBinding = Http(system).bindAndHandle(interface = host, port = port, handler = route).onSuccess {
    case _ => log.info(s"akka-http server started on $host:$port")
  }
}
