import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}
import ru.diemyst.HttpFrontend

import scala.concurrent.Await
import scala.concurrent.duration._

class FullTestKitExampleSpec extends WordSpec with Matchers with BeforeAndAfterAll with ScalaFutures {
  import akka.http.scaladsl.model.HttpMethods._

  implicit val testSystem =
    akka.actor.ActorSystem("test-system")
  import testSystem.dispatcher
  implicit val fm = ActorMaterializer()
  val server = new HttpFrontend("src\\test\\resources\\test_dir")

  override def beforeAll() = {
    //warmup
    Await.ready(sendRequest(HttpRequest(GET, uri = "/topic_list")), 2.second)
  }

  override def afterAll() = testSystem.shutdown()

  def sendRequest(req: HttpRequest) =
    Source.single(req).via(
      Http().outgoingConnection(host = "localhost",
        port = 8080)
    ).runWith(Sink.head)

  def includeSpec(req: HttpRequest, checkStr: String) = {
    whenReady(sendRequest(req)) { resp =>
      whenReady(Unmarshal(resp.entity).to[String]) { str =>
        str should include(checkStr)
      }
    }
  }

  "The app should return topiclist" in {
    includeSpec(HttpRequest(GET, uri = "/topic_list"), "[\"topic1\", \"topic2\", \"topic3\"]")
  }

  "The app should return last timestamp" in {
    includeSpec(HttpRequest(GET, uri = "/last_timestamp?topicname=topic1"), "2001-02-14-12-54-23")
  }

  "The app should return error" in {
    includeSpec(HttpRequest(GET, uri = "/last_timestamp?topicname=topic4"), "error")
  }

  "The app should return stats from topic2" in {
    includeSpec(HttpRequest(GET, uri = "/last_stats?topicname=topic2"), "\"sum\": 5,\n  \"min\": 342,\n  \"max\": 32427334,\n  \"average\": 6576763")
  }

  "The app should return data from csv from topic1" in {
    includeSpec(HttpRequest(GET, uri = "/last_run_partition_list?topicname=topic1"),
      "\"partList\": [[\"5536\", \"4323456\"], [\"32688\", \"4464995\"], [\"35\", \"4334123856\"], [\"435568\", \"76373\"], [\"34344\", \"171734\"]]")
  }


}
