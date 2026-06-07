package prism

import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{HttpCookie, RawHeader, SameSite, `Set-Cookie`}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class HeaderRuleSpec extends AnyWordSpec with Matchers {

  private def resp(hs: HttpHeader*): HttpResponse =
    HttpResponse(headers = hs.toList, entity = HttpEntity("x"))

  private def cookieOf(r: HttpResponse): HttpCookie = r.header[`Set-Cookie`].get.cookie
  private def valueOf(r: HttpResponse, name: String): Option[String] =
    r.headers.collectFirst { case h if h.lowercaseName == name.toLowerCase => h.value }

  "HeaderRule.CookieFlags" should {
    "enforce HttpOnly / Secure / SameSite on a plain cookie" in {
      val out = HeaderRule.mutate(
        resp(`Set-Cookie`(HttpCookie("s", "abc"))),
        HeaderRule.CookieFlags(Some(true), Some(true), Some("Lax"))
      )
      val c = cookieOf(out)
      c.httpOnly shouldBe true
      c.secure   shouldBe true
      c.sameSite shouldBe Some(SameSite.Lax)
    }
    "strip HttpOnly (test-harness direction)" in {
      val out = HeaderRule.mutate(
        resp(`Set-Cookie`(HttpCookie("s", "abc").withHttpOnly(true))),
        HeaderRule.CookieFlags(Some(false), None, None)
      )
      cookieOf(out).httpOnly shouldBe false
    }
    "leave a flag untouched when its option is None" in {
      val out = HeaderRule.mutate(
        resp(`Set-Cookie`(HttpCookie("s", "abc").withSecure(true))),
        HeaderRule.CookieFlags(None, None, None)
      )
      cookieOf(out).secure shouldBe true
    }
  }

  "HeaderRule.SetHeader / StripHeader" should {
    "add a header, replacing any existing one of the same name" in {
      val out = HeaderRule.mutate(
        resp(RawHeader("X-Frame-Options", "SAMEORIGIN")),
        HeaderRule.SetHeader("X-Frame-Options", "DENY")
      )
      out.headers.count(_.lowercaseName == "x-frame-options") shouldBe 1
      valueOf(out, "X-Frame-Options") shouldBe Some("DENY")
    }
    "strip a header" in {
      val out = HeaderRule.mutate(
        resp(RawHeader("X-Powered-By", "PHP/8")),
        HeaderRule.StripHeader("X-Powered-By")
      )
      out.headers.exists(_.lowercaseName == "x-powered-by") shouldBe false
    }
  }

  "HeaderRule.applyAll" should {
    "apply rules in order" in {
      val out = HeaderRule.applyAll(
        resp(`Set-Cookie`(HttpCookie("s", "x")), RawHeader("Server", "nginx")),
        List(
          HeaderRule.CookieFlags(Some(true), None, None),
          HeaderRule.StripHeader("Server"),
          HeaderRule.SetHeader("Content-Security-Policy", "default-src 'self'")
        )
      )
      cookieOf(out).httpOnly shouldBe true
      out.headers.exists(_.lowercaseName == "server") shouldBe false
      valueOf(out, "Content-Security-Policy") shouldBe Some("default-src 'self'")
    }
  }
}
