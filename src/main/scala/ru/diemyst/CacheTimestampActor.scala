package ru.diemyst

import java.io.File

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.pipe

import scala.concurrent.Future


/**
 * User: diemust
 * Date: 19.08.2015
 * Time: 20:49
 * Кэш, если вдруг огромное количество файлов. Можно такое же сделать для min, max, average
 * Еще идея была сделать мониторинг папки на WatchService, инфу обновлять походу, а на запросы всё из кэша доставать
 * (например из БД, если всего много)
 */
class CacheTimestampActor(var cachedDirs: Map[String, (String, Long)]) extends Actor with ActorLogging {
  implicit val exContext = context.dispatcher

  override def receive: Receive = {
    case GetLast(dir, topic) =>
      val topicOp = cachedDirs.get(topic)
      Future {
        val lastModify = new File(dir + File.separator + topic + File.separator + "history").lastModified()
        topicOp match {
          //если вообще нет файла, но в кэше он сохранен
          case Some((stamp, timeModify)) if lastModify == 0 =>
            ErrorMsg("no such topic")
          case None if lastModify == 0 =>
            ErrorMsg("no such topic")
          case Some((stamp, timeModify)) if lastModify > timeModify =>
            result(dir, topic, lastModify)
          case None =>
            result(dir, topic, lastModify)
          case Some((stamp, timeModify)) =>
            SuccessStamp(stamp)

        }
      } pipeTo sender

    case UpdateCache(topic, stamp, timeModify) =>
      cachedDirs += topic -> (stamp, timeModify)

    case DeleteCache(topic) =>
      cachedDirs - topic

    case _@msg =>
      log.debug("Case not implemented: " + msg)
  }

  def result(dir: String, topic: String, lastModify: Long): ResponseMessage = {
    val newStampOp = lastTimestamp(dir, topic)
    newStampOp match {
      case Some(newStamp) =>
        self ! UpdateCache(topic, newStamp, lastModify)
        SuccessStamp(newStamp)
      case None =>
        ErrorMsg("no timestamp directories")
    }
  }

  def lastTimestamp(dir: String, topicname: String): Option[String] = {
    val file = new File(dir + File.separator + topicname + File.separator + "history")
    Option(file.list()).filterNot(_.isEmpty).map{_.sorted.last}
  }
}

object CacheTimestampActor {
  def props = Props(classOf[CacheTimestampActor], Map.empty[String, (String, Long)]).withDispatcher("custom-thread-pool-dispatcher")
}

class ResponseMessage
case class SuccessStamp(stamp: String) extends ResponseMessage
case class ErrorMsg(errorMsg: String) extends ResponseMessage

case class GetLast(dir: String, topic: String)
case class UpdateCache(topic: String, stamp: String, timeModify: Long)
case class DeleteCache(topic: String)
