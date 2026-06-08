package prism

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{HttpCookie, RawHeader, `Set-Cookie`}
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.compiletime.uninitialized
import scala.concurrent.Await
import scala.concurrent.duration.*

/** Integration test: binds a stub origin and the real ProxyServer handler, then
 * drives it over real HTTP connections. */
class ProxyServerSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "proxy-server-spec", ConfigFactory.load())
  import system.executionContext

  // Stub origin: HTML on /, echoes received headers on /headers, sets a weak cookie
  // and a fingerprint header on /cookie.
  private val originRoute =
    concat(
      path("headers") {
        extractRequest(req => complete(req.headers.map(h => s"${h.name}: ${h.value}").mkString("\n")))
      },
      path("cookie") {
        respondWithHeaders(`Set-Cookie`(HttpCookie("s", "abc")), RawHeader("X-Powered-By", "PHP/8")) {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<html>internal</html>"))
        }
      },
      get(complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<html>internal origin</html>")))
    )

  private var origin: Http.ServerBinding = uninitialized
  private var proxy:  Http.ServerBinding = uninitialized
  private var port:   Int                = uninitialized

  override def beforeAll(): Unit = {
    origin = Await.result(Http().newServerAt("127.0.0.1", 0).bind(originRoute), 5.seconds)
    val cfg = ProxyConfig.from(
      ConfigFactory.parseString(s"""prism.proxy {
        origin = "http://127.0.0.1:${origin.localAddress.getPort}"
        rules = [
          { type = rewrite,      from = "internal", to = "EXTERNAL" }
          { type = cookie-flags, http-only = true, secure = true }
          { type = strip-header, name = "X-Powered-By" }
        ]
      }""").withFallback(ConfigFactory.load()).resolve().getConfig("prism.proxy")
    )
    proxy = Await.result(Http().newServerAt("127.0.0.1", 0).bind(ProxyServer.buildHandler(cfg)), 5.seconds)
    port  = proxy.localAddress.getPort
  }

  override def afterAll(): Unit = {
    Await.result(proxy.unbind(), 5.seconds)
    Await.result(origin.unbind(), 5.seconds)
    system.terminate()
    Await.result(system.whenTerminated, 5.seconds)
  }

  private def call(path: String): HttpResponse =
    Await.result(Http().singleRequest(HttpRequest(uri = s"http://127.0.0.1:$port$path")), 5.seconds)
  private def body(r: HttpResponse): String =
    Await.result(r.entity.toStrict(5.seconds), 5.seconds).data.utf8String

  "ProxyServer" should {
    "answer the health path without proxying" in {
      val r = call("/healthz")
      r.status shouldBe StatusCodes.OK
      body(r).trim shouldBe "ok"
    }

    "proxy and rewrite the response body" in {
      val r = call("/")
      r.status shouldBe StatusCodes.OK
      body(r) should include ("EXTERNAL origin")
    }

    "add X-Forwarded-* and Via on the upstream request" in {
      val echoed = body(call("/headers"))
      echoed.toLowerCase should include ("x-forwarded-for")
      echoed.toLowerCase should include ("x-forwarded-proto: http")
      echoed             should include ("Via: 1.1 prism")
    }

    "apply header rules to the response (enforce cookie flags, strip fingerprint)" in {
      val r = call("/cookie")
      val c = r.header[`Set-Cookie`].get.cookie
      c.httpOnly shouldBe true
      c.secure   shouldBe true
      r.headers.exists(_.lowercaseName == "x-powered-by") shouldBe false
    }

    "expose Prometheus metrics at the metrics path" in {
      call("/")                       // record at least one proxied request
      val r = call("/metrics")
      r.status shouldBe StatusCodes.OK
      val out = body(r)
      out should include ("# TYPE prism_requests_total counter")
      out should include ("prism_request_duration_seconds_count")
    }
  }

  "ProxyServer with a dead origin" should {
    "return 502 Bad Gateway, not a 500" in {
      val cfg = ProxyConfig.from(
        ConfigFactory.parseString("""prism.proxy { origin = "http://127.0.0.1:1" }""")
          .withFallback(ConfigFactory.load()).resolve().getConfig("prism.proxy")
      )
      val b = Await.result(Http().newServerAt("127.0.0.1", 0).bind(ProxyServer.buildHandler(cfg)), 5.seconds)
      val r = Await.result(
        Http().singleRequest(HttpRequest(uri = s"http://127.0.0.1:${b.localAddress.getPort}/")),
        10.seconds
      )
      r.status shouldBe StatusCodes.BadGateway
      Await.result(b.unbind(), 5.seconds)
    }
  }
}
