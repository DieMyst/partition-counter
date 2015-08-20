package ru.diemyst

import akka.actor.ActorSystem
import akka.dispatch.{PriorityGenerator, UnboundedPriorityMailbox}
import com.typesafe.config.Config

/**
 * User: diemust
 * Date: 19.08.2015
 * Time: 21:48
 */
class PrioMailbox(settings: ActorSystem.Settings, config: Config)
  extends UnboundedPriorityMailbox(
    PriorityGenerator {
      case UpdateCache => 4
      case DeleteCache => 3
      case otherwise     => 1
    })
