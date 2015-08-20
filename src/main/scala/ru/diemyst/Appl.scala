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

  if (args.length > 2) {
    println("to many arguments, bring first")
  }
  implicit val system = ActorSystem("test-system", ConfigFactory.load())
  new HttpFrontend(args(0))
}
