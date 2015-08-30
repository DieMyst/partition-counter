package ru.diemyst

import akka.actor.{Actor, Props}
import akka.pattern.{ask, pipe}
import akka.util.Timeout

import scala.concurrent.duration._

/**
 * User: diemust
 * Date: 30.08.2015
 * Time: 16:18
 */
class ManagerActor extends Actor {

  implicit val exContext = context.dispatcher
  implicit val timeout: Timeout = Timeout(60.seconds)

  val cacheActor = context.actorOf(CacheTimestampActor.props)
  val statsActor = context.actorOf(Props(classOf[StatsActor]))
  val listActor = context.actorOf(Props(classOf[ListActor]))

  override def receive: Receive = {
    case LastStatsRequest(dir, topicName) =>
      println("last stats m")
      (for {
        cache <- (cacheActor ? GetLast(dir, topicName)).mapTo[ResponseMessage]
        req = ResponseWrapper(cache, dir, topicName)
        stats <- statsActor ? req
      } yield stats) pipeTo sender
    case LastTimestampRequest(dir, topicName) =>
      (cacheActor ? GetLast(dir, topicName)) pipeTo sender
    case LastRunPartitionListRequest(dir, topicName) =>
      println("last run partition list")
      (for {
        cache <- (cacheActor ? GetLast(dir, topicName)).mapTo[ResponseMessage]
        req = ResponseWrapper(cache, dir, topicName)
        stats <- listActor ? req
      } yield stats) pipeTo sender
  }



}

object ManagerActor {
  def props: Props = Props(classOf[ManagerActor])
}

case class LastStatsRequest(dir: String, topicName: String)
case class LastTimestampRequest(dir: String, topicName: String)
case class LastRunPartitionListRequest(dir: String, topicName: String)

case class ResponseWrapper(resp: ResponseMessage, dir: String, topicName: String)
