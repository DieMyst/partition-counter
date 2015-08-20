package ru.diemyst

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory

/**
 * User: diemust
 * Date: 20.08.2015
 * Time: 3:53
 */
object Appl extends App {
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

  if (args.length > 2) {
    println("to many arguments, get first")
  }
  implicit val system = ActorSystem("test-system", ConfigFactory.load())
  new HttpFrontend(args(0))
}
