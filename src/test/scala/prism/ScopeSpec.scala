package prism

import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.Host
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ScopeSpec extends AnyWordSpec with Matchers {

  private def req(path: String, host: String = "example.com", method: HttpMethod = HttpMethods.GET): HttpRequest =
    HttpRequest(method = method, uri = path).withHeaders(Host(host))

  "Scope.globMatch" should {
    "treat a trailing * as a prefix and otherwise match exactly" in {
      Scope.globMatch("/a/*", "/a/b") shouldBe true
      Scope.globMatch("/a*", "/a")    shouldBe true
      Scope.globMatch("/a", "/a")     shouldBe true
      Scope.globMatch("/a", "/b")     shouldBe false
      Scope.globMatch("/a/*", "/b/c") shouldBe false
    }
  }

  "Scope.matches" should {
    "match everything when empty (Scope.any)" in {
      Scope.any.matches(req("/anything")) shouldBe true
    }
    "match by path glob" in {
      Scope(path = Some("/products/*")).matches(req("/products/9")) shouldBe true
      Scope(path = Some("/products/*")).matches(req("/cart"))       shouldBe false
    }
    "match by host, ignoring the port" in {
      val withPort = HttpRequest(uri = "/").withHeaders(Host("a.com", 8080))
      Scope(host = Some("a.com")).matches(withPort)                shouldBe true
      Scope(host = Some("a.com")).matches(req("/", host = "b.com")) shouldBe false
    }
    "match by method, case-insensitively" in {
      Scope(method = Some("post")).matches(req("/", method = HttpMethods.POST)) shouldBe true
      Scope(method = Some("post")).matches(req("/", method = HttpMethods.GET))  shouldBe false
    }
    "AND multiple conditions" in {
      val s = Scope(path = Some("/api/*"), method = Some("GET"))
      s.matches(req("/api/x"))                        shouldBe true
      s.matches(req("/api/x", method = HttpMethods.POST)) shouldBe false
      s.matches(req("/home"))                         shouldBe false
    }
  }
}
