package prism

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

class HtmlTextRewriteStageSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("html-text-rewrite-spec")
  override def afterAll(): Unit = Await.result(system.terminate(), 5.seconds)

  // Inner rewriter: a plain literal swap that, WITHOUT the tokenizer, would also
  // hit tag names / attributes / scripts. The tokenizer must confine it to text.
  private val inner = new LiteralRewriter(Seq("head" -> "HEAD", "internal" -> "EXTERNAL"))

  private def oneShot(s: String): String =
    Await.result(
      Source.single(ByteString(s)).via(HtmlTextRewriteFlow(inner)).runFold(ByteString.empty)(_ ++ _),
      5.seconds
    ).utf8String

  private def stream(s: String, size: Int): String =
    Await.result(
      Source(ByteString(s).grouped(size).toList).via(HtmlTextRewriteFlow(inner)).runFold(ByteString.empty)(_ ++ _),
      5.seconds
    ).utf8String

  "HtmlTextRewriteStage (text only)" should {
    "rewrite text content" in {
      oneShot("<p>go to head now</p>") shouldBe "<p>go to HEAD now</p>"
    }
    "NOT rewrite tag names" in {
      oneShot("<head><body>head</body></head>") shouldBe "<head><body>HEAD</body></head>"
    }
    "NOT rewrite attribute values" in {
      oneShot("""<a href="internal" title="head">internal</a>""") shouldBe
        """<a href="internal" title="head">EXTERNAL</a>"""
    }
    "tolerate '>' inside a quoted attribute value" in {
      oneShot("""<a title="a > head">head</a>""") shouldBe """<a title="a > head">HEAD</a>"""
    }
    "NOT rewrite inside <script>" in {
      oneShot("<script>var head = internal;</script>head") shouldBe
        "<script>var head = internal;</script>HEAD"
    }
    "NOT rewrite inside <style>" in {
      oneShot("<style>.head{}</style><p>head</p>") shouldBe "<style>.head{}</style><p>HEAD</p>"
    }
    "NOT rewrite inside comments" in {
      oneShot("<!-- head internal --><p>head</p>") shouldBe "<!-- head internal --><p>HEAD</p>"
    }
    "handle a bare '<' that is not a tag as text" in {
      oneShot("1 < 2 and head") shouldBe "1 < 2 and HEAD"
    }
    "not match a pattern across a tag boundary" in {
      // "head" is broken by the <b> tag → two text nodes, no match
      oneShot("he<b>ad</b> and head") shouldBe "he<b>ad</b> and HEAD"
    }
  }

  "HtmlTextRewriteStage (streaming)" should {
    val doc =
      """<!doctype html><html><head><title>head</title></head>
        |<body class="header">
        |  <!-- head internal comment -->
        |  <p>the head office is internal</p>
        |  <a href="http://internal/head" title="head">go to head</a>
        |  <script>var x = "head"; // internal</script>
        |  <style>.head { color: internal; }</style>
        |  he<b>ad</b> head&amp;internal head.
        |</body></html>""".stripMargin
    val expected = oneShot(doc)
    val bytes    = ByteString(doc)

    "match the single-shot result for ANY chunk boundary" in {
      for (size <- 1 to bytes.length) withClue(s"chunk size = $size: ") {
        stream(doc, size) shouldBe expected
      }
    }

    "actually have rewritten the visible text (sanity on the oracle)" in {
      expected should include ("<title>HEAD</title>")           // title text IS text → rewritten
      expected should include ("the HEAD office is EXTERNAL")   // body text rewritten
      expected should include ("""class="header"""")           // attribute untouched
      expected should include ("""<script>var x = "head"; // internal</script>""") // script untouched
    }
  }
}
