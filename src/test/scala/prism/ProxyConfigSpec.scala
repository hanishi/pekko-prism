package prism

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{HttpCookie, RawHeader, `Set-Cookie`}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

class ProxyConfigSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("proxy-config-spec")
  override def afterAll(): Unit = Await.result(system.terminate(), 5.seconds)

  /** Parse a HOCON snippet layered over the baked-in application.conf defaults. */
  private def parse(hocon: String): ProxyConfig =
    ProxyConfig.from(
      ConfigFactory.parseString(hocon).withFallback(ConfigFactory.load()).resolve()
        .getConfig("prism.proxy")
    )

  "ProxyConfig.from" should {
    "read the basics, falling back to defaults" in {
      val c = parse("""prism.proxy { origin = "http://up:9000" }""")
      c.origin.toString shouldBe "http://up:9000"
      c.port       shouldBe 8080
      c.interface  shouldBe "0.0.0.0"
      c.healthPath shouldBe "/healthz"
      c.textOnly   shouldBe false
      c.tls        shouldBe None
      c.rules      shouldBe empty
      c.headerRules shouldBe empty
    }

    "parse TLS when enabled" in {
      val c = parse("""prism.proxy { origin="http://u", tls { enabled=true, keystore="k.p12", password="pw" } }""")
      c.tls shouldBe Some(("k.p12", "pw"))
    }

    "split body rules from header rules" in {
      val c = parse("""prism.proxy {
        origin = "http://u"
        rules = [
          { type = rewrite,       from = "a", to = "b" }
          { type = rewrite-word,  from = "c", to = "d" }
          { type = wrap-url,      anchor = "https://t", template = "https://fp?{enc}" }
          { type = insert-before, anchor = "</head>", html = "X" }
          { type = insert-after,  anchor = "<body>",  html = "Y" }
          { type = cookie-flags,  http-only = true }
          { type = set-header,    name = "CSP", value = "x" }
          { type = strip-header,  name = "Server" }
        ]
      }""")
      c.rules.size shouldBe 5
      c.rules should contain allOf (
        Rule.Rewrite("a", "b"),
        Rule.RewriteWord("c", "d"),
        Rule.WrapUrl("https://t", "https://fp?{enc}"),
        Rule.InsertBefore("</head>", "X"),
        Rule.InsertAfter("<body>", "Y")
      )
      c.headerRules.size shouldBe 3
      c.headerRules should contain allOf (
        HeaderRule.CookieFlags(Some(true), None, None),
        HeaderRule.SetHeader("CSP", "x"),
        HeaderRule.StripHeader("Server")
      )
    }

    "reject an unknown rule type" in {
      an[Exception] should be thrownBy parse("""prism.proxy { origin="http://u", rules=[{type=bogus}] }""")
    }

    "require a non-empty origin" in {
      an[Exception] should be thrownBy parse("""prism.proxy { origin = "" }""")
    }
  }

  "ProxyConfig.accept" should {
    def ct(s: String): ContentType = ContentType.parse(s).toOption.get

    "default to HTML only" in {
      val a = parse("""prism.proxy { origin="http://u" }""").accept
      a(ContentTypes.`text/html(UTF-8)`)  shouldBe true
      a(ContentTypes.`application/json`)  shouldBe false
    }
    "admit XML when configured" in {
      val a = parse("""prism.proxy { origin="http://u", accept=["html","xml"] }""").accept
      a(ct("application/xml; charset=utf-8")) shouldBe true
      a(ContentTypes.`application/json`)      shouldBe false
    }
    "admit everything with all" in {
      val a = parse("""prism.proxy { origin="http://u", accept=["all"] }""").accept
      a(ContentTypes.`application/json`) shouldBe true
    }
  }

  "ProxyConfig.rewriteFlow" should {
    def run(c: ProxyConfig, in: String): String =
      Await.result(
        Source.single(ByteString(in)).via(c.rewriteFlow).runFold(ByteString.empty)(_ ++ _),
        5.seconds
      ).utf8String

    "apply rewrite then insert, in order" in {
      val c = parse("""prism.proxy {
        origin = "http://u"
        rules = [
          { type = rewrite,       from = "internal", to = "EXTERNAL" }
          { type = insert-before, anchor = "</head>", html = "<meta>" }
        ]
      }""")
      run(c, "<head>internal</head>") shouldBe "<head>EXTERNAL<meta></head>"
    }

    "confine rewrites to text nodes when text-only" in {
      val c = parse("""prism.proxy {
        origin = "http://u", text-only = true
        rules = [ { type = rewrite, from = "head", to = "HEAD" } ]
      }""")
      run(c, "<head>head</head>") shouldBe "<head>HEAD</head>"
    }

    "be identity with no rules" in {
      run(parse("""prism.proxy { origin="http://u" }"""), "<p>x</p>") shouldBe "<p>x</p>"
    }
  }

  "ProxyConfig scoped rules" should {
    def runFor(c: ProxyConfig, req: HttpRequest, in: String): String =
      Await.result(
        Source.single(ByteString(in)).via(c.rewriteFlowFor(req)).runFold(ByteString.empty)(_ ++ _),
        5.seconds
      ).utf8String

    "apply a `when`-scoped rule only on matching requests" in {
      val c = parse("""prism.proxy {
        origin = "http://u"
        rules = [ { type = rewrite, from = "internal", to = "EXTERNAL", when { path = "/api/*" } } ]
      }""")
      c.hasScopes shouldBe true
      runFor(c, HttpRequest(uri = "/api/x"), "internal") shouldBe "EXTERNAL" // in scope
      runFor(c, HttpRequest(uri = "/home"),  "internal") shouldBe "internal" // out of scope: untouched
    }
  }

  "ProxyConfig.applyHeaderRules" should {
    "enforce cookie flags and strip a header" in {
      val c = parse("""prism.proxy {
        origin = "http://u"
        rules = [
          { type = cookie-flags, http-only = true, secure = true }
          { type = strip-header, name = "Server" }
        ]
      }""")
      val resp = HttpResponse(
        headers = List(`Set-Cookie`(HttpCookie("s", "x")), RawHeader("Server", "nginx")),
        entity  = HttpEntity("ok")
      )
      val out    = c.applyHeaderRules(resp)
      val cookie = out.header[`Set-Cookie`].get.cookie
      cookie.httpOnly shouldBe true
      cookie.secure   shouldBe true
      out.headers.exists(_.lowercaseName == "server") shouldBe false
    }
  }
}
