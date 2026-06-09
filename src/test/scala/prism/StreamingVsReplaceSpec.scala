package prism

import org.apache.pekko.util.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/**
 * The one thing `String.replace` fundamentally cannot do: match a pattern that straddles
 * a chunk boundary. Replacing each chunk independently misses it; the streaming rewriter
 * stitches it via the carry. This is the reason the engine exists.
 */
class StreamingVsReplaceSpec extends AnyWordSpec with Matchers {

  // "internal.example.com" is split across the two chunks.
  private val chunks = Seq("...href=\"http://internal.examp", "le.com/x\"...")
  private val rw     = BmhRewriter("internal.example.com", "X")

  /** Drive a rewriter through the streaming envelope (carry + flush), as RewriteStage does. */
  private def streamRewrite(parts: Seq[String]): String = {
    var carry = ByteString.empty
    val out   = ByteString.newBuilder
    parts.foreach { p =>
      carry ++= ByteString(p)
      val (o, n) = rw(carry, atEOF = false)
      out ++= o
      carry = carry.drop(n)
    }
    val (o, _) = rw(carry, atEOF = true)
    out ++= o
    out.result().utf8String
  }

  "naive per-chunk String.replace" should {
    "MISS a match that straddles a chunk boundary" in {
      val joined = chunks.map(_.replace("internal.example.com", "X")).mkString
      joined should include ("internal.example.com") // never matched: still there
      joined should not include ("X")
    }
  }

  "the streaming rewriter" should {
    "catch the same straddling match via the carry" in {
      val joined = streamRewrite(chunks)
      joined should not include ("internal.example.com")
      joined should include ("X")
    }
  }
}
