package ru.diemyst

import java.io.File

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._

/**
 * User: diemust
 * Date: 20.08.2015
 * Time: 3:53
 */
object Appl extends App with HttpFrontend {

  if (args.isEmpty) {
    println("no  arguments")
    System.exit(1)
  }

  println(
    """/topic_list for get list of topics
      |/last_timestamp?topicname= for last timestamp of topic
      |/last_stats?topicsname= for stats of last timestamp of topic
      |/last_run_partition_list?topicname= for list of last run partitions
    """.stripMargin)

  if (args.length > 1) {
    println("to many arguments, get first")
  }

  override implicit val system = ActorSystem("test-system", ConfigFactory.load())
  override implicit val timeout: Timeout = Timeout(60.seconds)
  implicit val materializer = ActorMaterializer()
  override implicit val dir = args(0)

  assert(new File(dir).exists())

  override val managerActor = system.actorOf(ManagerActor.props)

  Http(system).bindAndHandle(route, host, port)
}
