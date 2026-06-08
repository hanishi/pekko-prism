package prism

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.atomic.AtomicReference
import scala.compiletime.uninitialized
import scala.concurrent.Await
import scala.concurrent.duration.*

/** Hot reload: the handler reads the config supplier per request, so swapping the
 * live config changes behaviour without rebinding. */
class ReloadSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "reload-spec", ConfigFactory.load())

  private var origin: Http.ServerBinding = uninitialized
  private var proxy:  Http.ServerBinding = uninitialized
  private var port:   Int                = uninitialized
  private val cfgRef = new AtomicReference[ProxyConfig]()

  private def cfgWith(rule: String): ProxyConfig =
    ProxyConfig.from(
      ConfigFactory.parseString(
        s"""prism.proxy { origin = "http://127.0.0.1:${origin.localAddress.getPort}", rules = [ $rule ] }"""
      ).withFallback(ConfigFactory.load()).resolve().getConfig("prism.proxy")
    )

  override def beforeAll(): Unit = {
    origin = Await.result(
      Http().newServerAt("127.0.0.1", 0).bind(
        get(complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<html>internal origin</html>")))),
      5.seconds)
    cfgRef.set(cfgWith("""{ type = rewrite, from = "internal", to = "FIRST" }"""))
    proxy = Await.result(
      Http().newServerAt("127.0.0.1", 0).bind(ProxyServer.handlerFor(() => cfgRef.get, new Metrics())),
      5.seconds)
    port = proxy.localAddress.getPort
  }

  override def afterAll(): Unit = {
    Await.result(proxy.unbind(), 5.seconds)
    Await.result(origin.unbind(), 5.seconds)
    system.terminate()
    Await.result(system.whenTerminated, 5.seconds)
  }

  private def fetch(): String = {
    val r = Await.result(Http().singleRequest(HttpRequest(uri = s"http://127.0.0.1:$port/")), 5.seconds)
    Await.result(r.entity.toStrict(5.seconds), 5.seconds).data.utf8String
  }

  "handlerFor over a live config ref" should {
    "apply the swapped-in rule without rebinding" in {
      fetch() should include ("FIRST origin")
      cfgRef.set(cfgWith("""{ type = rewrite, from = "internal", to = "SECOND" }"""))
      fetch() should include ("SECOND origin")
    }
  }
}
