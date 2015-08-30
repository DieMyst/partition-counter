import akka.http.scaladsl.model.ContentTypes._
import akka.http.scaladsl.model.StatusCodes._
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import akka.util.Timeout
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import ru.diemyst._

import scala.concurrent.duration._

class HttpFrontendSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures with ScalatestRouteTest with HttpFrontend {

  implicit val testSystem =
    akka.actor.ActorSystem("test-system")
  implicit val fm = ActorMaterializer()
  implicit val timeout: Timeout = Timeout(60.seconds)
  override val dir = "src\\test\\resources\\test_dir"
  override val managerActor = system.actorOf(ManagerActor.props)

  "The app should return topiclist" in {
    Get("/topic_list") ~> route ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      responseAs[TopicsListResult] shouldBe TopicsListResult(List("topic1", "topic2", "topic3"))
    }
  }

  "The app should return last timestamp" in {
    Get("/last_timestamp?topicname=topic1") ~> route ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      responseAs[LastTimestampResult] shouldBe LastTimestampResult("2001-02-14-12-54-23")
    }
  }

  "The app should return error" in {
    Get("/last_timestamp?topicname=topic4") ~> route ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      responseAs[ru.diemyst.ErrorResult] shouldBe ru.diemyst.ErrorResult("no such topic")
    }
  }

  "The app should return stats from topic2" in {
    Get("/last_stats?topicname=topic2") ~> route ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      responseAs[LastTimestampStatsResult] shouldBe LastTimestampStatsResult(5, 342, 32427334, 6576763)
    }
  }

  "The app should return data from csv from topic1" in {
    Get("/last_run_partition_list?topicname=topic1") ~> route ~> check {
      status shouldBe OK
      contentType shouldBe `application/json`
      responseAs[LastRunPartitionListResult] shouldBe LastRunPartitionListResult(List(("5536", "4323456"), ("32688", "4464995"),
        ("35", "4334123856"), ("435568", "76373"),
        ("34344", "171734")))
    }
  }
}
