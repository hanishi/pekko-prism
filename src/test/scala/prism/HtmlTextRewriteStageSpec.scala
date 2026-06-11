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
    "NOT treat custom elements whose name starts with script/style as raw text" in {
      // The name must END after "script"/"style" (whitespace, '/', '>'); a '-' or
      // digit means a different element, whose text content is still rewritten.
      oneShot("<script-foo>head</script-foo>head") shouldBe "<script-foo>HEAD</script-foo>HEAD"
      oneShot("<script2>head</script2>head")       shouldBe "<script2>HEAD</script2>HEAD"
      oneShot("<style-x>head</style-x>head")       shouldBe "<style-x>HEAD</style-x>HEAD"
      // sanity: a real script with attributes is still raw text
      oneShot("""<script type="t">head</script>head""") shouldBe
        """<script type="t">head</script>HEAD"""
    }
    "treat a tag name longer than the script/style probe as a generic tag" in {
      // 8-letter name exceeds the probe cap: classified generic before the name ends
      oneShot("<scripted>head</scripted>head") shouldBe "<scripted>HEAD</scripted>HEAD"
    }
    "handle an absurdly long tag name (capped probe, bounded carry)" in {
      val tag = "a" * 100000
      stream(s"<$tag x=y>head</$tag> head", 1024) shouldBe s"<$tag x=y>HEAD</$tag> HEAD"
    }
    "classify script/style correctly when the name straddles ANY chunk boundary" in {
      val docs = List(
        "<script>head</script>head",         // raw-text element: inner text untouched
        "<scripts>head</scripts>head",       // one letter past "script": generic tag
        "<script-foo>head</script-foo>head", // name continues past a non-letter: generic
        "<style>head</style>head",
        "<styled>head</styled>head"
      )
      for (doc <- docs; size <- 1 to doc.length) withClue(s"doc=$doc chunk=$size: ") {
        stream(doc, size) shouldBe oneShot(doc)
      }
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
