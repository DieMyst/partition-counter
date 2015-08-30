package ru.diemyst

import java.io.File

import akka.actor.{ActorRef, ActorSystem}
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.pattern.ask
import akka.util.Timeout
import spray.json.DefaultJsonProtocol

import scala.concurrent.Future

/**
 * User: diemust
 * Date: 18.08.2015
 * Time: 21:33
 */
case class TopicsListResult(topics: List[String])
case class LastTimestampResult(timestamp: String)
case class LastTimestampStatsResult(sum: Long, min: Long, max: Long, average: Long)
case class LastRunPartitionListResult(partList: List[(String, String)])
case class ErrorResult(error: String)

case class FoldResult(min: Long, max: Long, sum: Long, count: Int) {
  def toStats: LastTimestampStatsResult = LastTimestampStatsResult(count, min, max, sum / count)
}

trait Protocols extends DefaultJsonProtocol with SprayJsonSupport {
  implicit def topicListFormat = jsonFormat1(TopicsListResult.apply)
  implicit def lastTimestampFormat = jsonFormat1(LastTimestampResult.apply)
  implicit def lastTimestampStatsFormat = jsonFormat4(LastTimestampStatsResult.apply)
  implicit def lastRunPartitionListFormat = jsonFormat1(LastRunPartitionListResult.apply)
  implicit def errorFormat = jsonFormat1(ErrorResult.apply)
}

trait HttpFrontend extends Protocols {

  import akka.http.scaladsl.model.MediaTypes._

  import scala.concurrent.ExecutionContext.Implicits.global

  implicit val system: ActorSystem
  implicit val timeout: Timeout

  val dir: String
  val managerActor: ActorRef

  val host = "127.0.0.1"
  val port = 8080

  val route: Route = {
    get {
      mapResponseEntity(_.withContentType(`application/json`)) {
        pathPrefix("topic_list") {
          complete {
            Future {
              Option(new File(dir).listFiles()).filter(_.nonEmpty) match {
                case Some(arr) => Right(TopicsListResult(arr.filter(_.isDirectory).map(_.getName).toList))
                case None => Left(ErrorResult("No topics"))
              }
            }
          }
        } ~
        parameter('topicname) { topicname =>
          path("last_timestamp") {
            complete {
              (managerActor ? LastTimestampRequest(dir, topicname))
                .map{
                  case SuccessStamp(stamp) => Right(LastTimestampResult(stamp))
                  case ErrorMsg(msg) => Left(ErrorResult(msg))
                }
            }
          } ~
          path("last_stats") {
            complete {
              (managerActor ? LastStatsRequest(dir, topicname)).map {
                case m: LastTimestampStatsResult => Right(m)
                case m: ErrorResult => Left(m)
              }
            }
          } ~
          path("last_run_partition_list") {
            complete {
              (managerActor ? LastRunPartitionListRequest(dir, topicname)).map {
                case m: LastRunPartitionListResult => Right(m)
                case m: ErrorResult => Left(m)
              }
            }
          }
        }
      }
    }
  }


}
