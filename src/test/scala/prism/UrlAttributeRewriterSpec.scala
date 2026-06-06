package prism

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

class UrlAttributeRewriterSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("url-rewriter-spec")
  override def afterAll(): Unit = Await.result(system.terminate(), 5.seconds)

  // Uppercase the value so changes are obvious and structure is easy to verify.
  private val rw = new UrlAttributeRewriter(
    anchors = Seq("href", "src"),
    transform = _.toUpperCase
  )

  /** Single-shot oracle: what the rewriter produces given the whole input at once. */
  private def oneShot(s: String): String = rw(ByteString(s), atEOF = true)._1.utf8String

  private def stream(chunks: Seq[ByteString]): String =
    Await.result(
      Source(chunks.toList).via(RewriteFlow(rw)).runFold(ByteString.empty)(_ ++ _),
      5.seconds
    ).utf8String

  "UrlAttributeRewriter (single-shot)" should {
    "rewrite double-quoted values, preserving quotes" in {
      oneShot("""<a href="http://h/x">""") shouldBe """<a href="HTTP://H/X">"""
    }
    "rewrite single-quoted values" in {
      oneShot("""<a href='http://h/x'>""") shouldBe """<a href='HTTP://H/X'>"""
    }
    "rewrite unquoted values up to whitespace or '>'" in {
      oneShot("<a href=/path/x class=z>") shouldBe "<a href=/PATH/X class=z>"
      oneShot("<a href=/path/x>")         shouldBe "<a href=/PATH/X>"
    }
    "tolerate whitespace around '='" in {
      oneShot("""<a href = "y">""") shouldBe """<a href = "Y">"""
    }
    "not touch a bare anchor word that is not an attribute" in {
      oneShot("see href in the docs") shouldBe "see href in the docs"
    }
    "respect name boundaries (data-href is not href)" in {
      oneShot("""<x data-href="y">""") shouldBe """<x data-href="y">"""
    }
    "emit verbatim (no corruption) when a quote is unterminated at EOF" in {
      oneShot("""<a href="http://unterminated""") shouldBe """<a href="http://unterminated"""
    }
    "rewrite multiple attributes in one document" in {
      oneShot("""<a href="u1"><img src='u2'>""") shouldBe """<a href="U1"><img src='U2'>"""
    }
  }

  "UrlAttributeRewriter (streaming)" should {
    val doc =
      """<html><head></head><body>
        |<a href="http://internal/a">one</a>
        |<img src='http://internal/b.png'/>
        |<a href=/relative/c >three</a>
        |</body></html>""".stripMargin
    val bytes    = ByteString(doc)
    val expected = oneShot(doc)

    "match the single-shot result for ANY chunk boundary" in {
      for (size <- 1 to bytes.length) withClue(s"chunk size = $size: ") {
        stream(bytes.grouped(size).toList) shouldBe expected
      }
    }

    "transform a value whose closing quote arrives in a later chunk" in {
      stream(List(ByteString("""<a href="http://int"""), ByteString("""ernal/x">end"""))) shouldBe
        """<a href="HTTP://INTERNAL/X">end"""
    }
  }

  "UrlAttributeRewriter (case-insensitive anchors)" should {
    val ci = new UrlAttributeRewriter(
      anchors = Seq("href"),
      transform = _.toUpperCase,
      caseInsensitive = true
    )
    def shot(s: String): String = ci(ByteString(s), atEOF = true)._1.utf8String

    "match anchors regardless of case" in {
      shot("""<a HREF="http://h/x">""") shouldBe """<a HREF="HTTP://H/X">"""
      shot("""<a Href="http://h/x">""") shouldBe """<a Href="HTTP://H/X">"""
    }
    "still respect name boundaries" in {
      shot("""<x data-HREF="y">""") shouldBe """<x data-HREF="y">"""
    }
    "leave structure byte-for-byte, folding only the match" in {
      // the anchor name 'HREF' is preserved verbatim; only the value changes
      shot("""<a HREF='abc'>""") shouldBe """<a HREF='ABC'>"""
    }
  }

  "UrlAttributeRewriter (entity decoding)" should {
    val rh = UrlAttributeRewriter.replacingHost("internal.example.com", "localhost")
    def shot(s: String): String = rh(ByteString(s), atEOF = true)._1.utf8String

    "see through &amp; so the host is matched in a query string" in {
      shot("""<a href="http://internal.example.com/p?a=1&amp;b=2">""") shouldBe
        """<a href="http://localhost/p?a=1&b=2">"""
    }
    "decode numeric references too" in {
      // &#46; is '.', so the dotted host is still recognised
      shot("""<a href="http://internal&#46;example&#46;com/x">""") shouldBe
        """<a href="http://localhost/x">"""
    }
    "preserve the original encoding when nothing is replaced" in {
      // no host match → emit the source bytes verbatim, entities intact
      shot("""<a href="http://other.test/p?a=1&amp;b=2">""") shouldBe
        """<a href="http://other.test/p?a=1&amp;b=2">"""
    }
    "match the host case-insensitively (replacingHost folds anchors)" in {
      shot("""<a HREF="http://internal.example.com/x">""") shouldBe
        """<a HREF="http://localhost/x">"""
    }
  }

  "UrlAttributeRewriter (overflow safety)" should {
    "stop scanning and emit verbatim past the value-length budget" in {
      val small = new UrlAttributeRewriter(Seq("href"), transform = _.toUpperCase, maxValueLength = 16)
      val long  = "x" * 100
      val in    = s"""<a href="$long">"""
      // value exceeds budget -> left untouched, no buffering blowup
      small(ByteString(in), atEOF = true)._1.utf8String shouldBe in
    }
  }
}
