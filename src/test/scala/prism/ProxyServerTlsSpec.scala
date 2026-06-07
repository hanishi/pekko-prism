package prism

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.nio.file.Files
import java.security.SecureRandom
import java.security.cert.X509Certificate
import javax.net.ssl.{SSLContext, TrustManager, X509TrustManager}
import scala.compiletime.uninitialized
import scala.concurrent.Await
import scala.concurrent.duration.*

class ProxyServerTlsSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "proxy-tls-spec", ConfigFactory.load())
  import system.executionContext

  private val dir      = Files.createTempDirectory("prism-tls")
  private val keystore = dir.resolve("proxy.p12").toString

  private def cfgFrom(hocon: String): ProxyConfig =
    ProxyConfig.from(
      ConfigFactory.parseString(hocon).withFallback(ConfigFactory.load()).resolve().getConfig("prism.proxy")
    )

  // A client that trusts any cert (the test keystore is self-signed).
  private val trustAllClient = {
    val ctx = SSLContext.getInstance("TLS")
    ctx.init(null, Array[TrustManager](new X509TrustManager {
      def getAcceptedIssuers: Array[X509Certificate]                       = Array.empty
      def checkClientTrusted(c: Array[X509Certificate], a: String): Unit   = ()
      def checkServerTrusted(c: Array[X509Certificate], a: String): Unit   = ()
    }), new SecureRandom())
    ConnectionContext.httpsClient(ctx)
  }

  private var origin: Http.ServerBinding = uninitialized
  private var proxy:  Http.ServerBinding = uninitialized
  private var port:   Int                = uninitialized

  override def beforeAll(): Unit = {
    // self-signed PKCS12 keystore via keytool (always present in a JDK)
    val keytool = System.getProperty("java.home") + "/bin/keytool"
    val p = new ProcessBuilder(
      keytool, "-genkeypair", "-keystore", keystore, "-storetype", "PKCS12",
      "-storepass", "changeit", "-keyalg", "RSA", "-keysize", "2048", "-validity", "365",
      "-alias", "proxy", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost,ip:127.0.0.1"
    ).redirectErrorStream(true).start()
    require(p.waitFor() == 0, "keytool failed to create the test keystore")

    origin = Await.result(
      Http().newServerAt("127.0.0.1", 0)
        .bind(get(complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "<html>internal</html>")))),
      5.seconds
    )
    val cfg = cfgFrom(s"""prism.proxy {
      origin = "http://127.0.0.1:${origin.localAddress.getPort}"
      tls { enabled = true, keystore = "$keystore", password = "changeit" }
      rules = [ { type = rewrite, from = "internal", to = "EXTERNAL" } ]
    }""")
    val ctx = ProxyServer.httpsContext(cfg).get
    proxy = Await.result(
      Http().newServerAt("localhost", 0).enableHttps(ctx).bind(ProxyServer.buildHandler(cfg)),
      5.seconds
    )
    port = proxy.localAddress.getPort
  }

  override def afterAll(): Unit = {
    Await.result(proxy.unbind(), 5.seconds)
    Await.result(origin.unbind(), 5.seconds)
    system.terminate()
    Await.result(system.whenTerminated, 5.seconds)
    Files.deleteIfExists(java.nio.file.Path.of(keystore))
    Files.deleteIfExists(dir)
  }

  "ProxyServer.httpsContext" should {
    "be None when tls is disabled" in {
      ProxyServer.httpsContext(cfgFrom("""prism.proxy { origin="http://u" }""")) shouldBe None
    }
    "be defined when tls points at a valid keystore" in {
      ProxyServer.httpsContext(cfgFrom(s"""prism.proxy {
        origin="http://u", tls { enabled=true, keystore="$keystore", password="changeit" } }""")) should not be empty
    }
  }

  "ProxyServer over HTTPS" should {
    "serve TLS and rewrite the body" in {
      val r = Await.result(
        Http().singleRequest(HttpRequest(uri = s"https://localhost:$port/"), connectionContext = trustAllClient),
        5.seconds
      )
      r.status shouldBe StatusCodes.OK
      Await.result(r.entity.toStrict(5.seconds), 5.seconds).data.utf8String should include ("EXTERNAL")
    }
  }
}
