package prism

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

class TokenRewriterSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("token-rewriter-spec")
  override def afterAll(): Unit = Await.result(system.terminate(), 5.seconds)

  private def oneShot(rw: TokenRewriter, s: String): String =
    rw(ByteString(s), atEOF = true)._1.utf8String

  private def stream(rw: TokenRewriter, chunks: Seq[ByteString]): String =
    Await.result(
      Source(chunks.toList).via(RewriteFlow(rw)).runFold(ByteString.empty)(_ ++ _),
      5.seconds
    ).utf8String

  "TokenRewriter (capture)" should {
    val upper = new TokenRewriter(Seq("https://"), transform = _.toUpperCase)

    "capture a URL and transform it, stopping at the boundary" in {
      oneShot(upper, """see https://h/x and stop""") shouldBe """see HTTPS://H/X and stop"""
    }
    "stop at a CDATA close bracket" in {
      oneShot(upper, "<![CDATA[https://h/x]]>") shouldBe "<![CDATA[HTTPS://H/X]]>"
    }
    "stop at a quote" in {
      oneShot(upper, """src="https://h/x"""") shouldBe """src="HTTPS://H/X""""
    }
    "capture to EOF when there is no boundary" in {
      oneShot(upper, "tail https://h/x") shouldBe "tail HTTPS://H/X"
    }
  }

  "TokenRewriter.wrappingUrls (capture-what-was-there)" should {
    val rw = TokenRewriter.wrappingUrls(
      "https://tracker.example.com",
      "https://fp.publisher.com/collect?dest={enc}"
    )
    "embed the URL-encoded original into a first-party URL" in {
      oneShot(rw, "<![CDATA[https://tracker.example.com/imp?cb=1]]>") shouldBe
        "<![CDATA[https://fp.publisher.com/collect?dest=" +
        "https%3A%2F%2Ftracker.example.com%2Fimp%3Fcb%3D1]]>"
    }
    "leave non-matching URLs alone" in {
      oneShot(rw, "<![CDATA[https://cdn.vendor.com/ad.mp4]]>") shouldBe
        "<![CDATA[https://cdn.vendor.com/ad.mp4]]>"
    }
  }

  "TokenRewriter (streaming)" should {
    val rw = TokenRewriter.wrappingUrls("https://t.example", "https://fp/c?d={enc}")
    val doc =
      """<VAST><Impression><![CDATA[https://t.example/a?x=1]]></Impression>""" +
      """<Tracking><![CDATA[https://t.example/b]]></Tracking>""" +
      """<MediaFile><![CDATA[https://other/v.mp4]]></MediaFile></VAST>"""
    val bytes    = ByteString(doc)
    val expected = oneShot(rw, doc)

    "match the single-shot result for ANY chunk boundary" in {
      for (size <- 1 to bytes.length) withClue(s"chunk size = $size: ") {
        stream(rw, bytes.grouped(size).toList) shouldBe expected
      }
    }

    "capture a URL whose boundary arrives in a later chunk" in {
      stream(rw, List(ByteString("x https://t.example/a"), ByteString("bc]end"))) shouldBe
        "x https://fp/c?d=" + java.net.URLEncoder.encode("https://t.example/abc", "UTF-8") + "]end"
    }
  }
}
