package ru.diemyst

import java.io.File

import akka.actor.Actor
import akka.pattern.pipe
import akka.stream.ActorMaterializer
import akka.stream.io.{Framing, SynchronousFileSource}
import akka.stream.scaladsl.Sink
import akka.util.ByteString

import scala.concurrent.Future

/**
 * User: diemust
 * Date: 30.08.2015
 * Time: 15:06
 */
abstract class BusinessActor extends Actor {
  implicit val exContext = context.dispatcher
  implicit val am = ActorMaterializer()

  val framing = Framing.delimiter(ByteString("\r\n"), maximumFrameLength = 100, allowTruncation = true)
  def getCsvName(dir: String, topicname: String, stamp: String) = dir + File.separator + topicname + File.separator + "history" + File.separator + stamp + File.separator + "offsets.csv"
}

class StatsActor extends BusinessActor {

  override def receive: Receive = {
    case ResponseWrapper(resp, dir, topicName) =>
      resp match {
        case SuccessStamp(stamp) =>
          getStats(getCsvName(dir, topicName, stamp)).pipeTo(sender())
        case ErrorMsg(msg) =>
          sender ! ErrorResult(msg)
      }
  }

  def getStats(path: String): Future[LastTimestampStatsResult] = {
    SynchronousFileSource(new File(path))
      .via(framing)
      .map(_.utf8String)
      .map(_.split(",")(1).toLong)
      .runWith(Sink.fold(FoldResult(Long.MaxValue, 0, 0, 0)) {
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
}

class ListActor extends BusinessActor {

  override def receive: Receive = {
    case ResponseWrapper(resp, dir, topicName) =>
      resp match {
        case SuccessStamp(stamp) =>
          getList(getCsvName(dir, topicName, stamp)).pipeTo(sender())
        case ErrorMsg(msg) =>
          sender ! ErrorResult(msg)
      }
  }

  def getList(path: String): Future[LastRunPartitionListResult] = {
    //здесь используется свой thread-pool для io
    SynchronousFileSource(new File(path))
      .via(framing)
      .map(_.utf8String)
      .map{str =>
      val arr = str.split(",")
      (arr(0), arr(1))
    }.runWith(Sink.fold(List.empty[(String, String)]){case (list, tuple) => list :+ tuple})
      .map(LastRunPartitionListResult)
  }
}
