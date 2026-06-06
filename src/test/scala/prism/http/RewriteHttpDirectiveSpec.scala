package prism.http

import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.complete
import org.apache.pekko.http.scaladsl.testkit.ScalatestRouteTest
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import prism.LiteralRewriter

class RewriteHttpDirectiveSpec extends AnyWordSpec with Matchers with ScalatestRouteTest {

  private val rewriter = new LiteralRewriter(Seq("internal" -> "localhost"))

  "RewriteHttp.rewriteHtmlResponses" should {

    "rewrite an HTML response from the inner route" in {
      val route = RewriteHttp.rewriteHtmlResponses(rewriter) {
        complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, "go to internal"))
      }
      Get() ~> route ~> check {
        responseAs[String] shouldBe "go to localhost"
      }
    }

    "leave a JSON response untouched even if it contains the pattern" in {
      val route = RewriteHttp.rewriteHtmlResponses(rewriter) {
        complete(HttpEntity(ContentTypes.`application/json`, """{"host":"internal"}"""))
      }
      Get() ~> route ~> check {
        responseAs[String] shouldBe """{"host":"internal"}"""
      }
    }
  }
}
