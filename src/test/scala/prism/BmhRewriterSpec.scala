package prism

import org.apache.pekko.util.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class BmhRewriterSpec extends AnyWordSpec with Matchers {

  /** Drive a rewriter through the streaming envelope at a fixed chunk size. */
  private def stream(rw: Rewriter, input: ByteString, chunk: Int): String = {
    var carry = ByteString.empty
    val out   = ByteString.newBuilder
    input.grouped(chunk).foreach { c =>
      carry ++= c
      val (o, n) = rw(carry, atEOF = false)
      out ++= o
      carry = carry.drop(n)
    }
    val (o, _) = rw(carry, atEOF = true)
    out ++= o
    out.result().utf8String
  }

  private def oneShot(rw: Rewriter, s: String): String = rw(ByteString(s), atEOF = true)._1.utf8String

  "BmhRewriter" should {
    val rw = BmhRewriter("internal.example.com", "www.example.com")

    "replace all occurrences in one pass" in {
      oneShot(rw, "a internal.example.com b internal.example.com c") shouldBe
        "a www.example.com b www.example.com c"
    }

    "leave non-matching input untouched (zero-copy path)" in {
      oneShot(rw, "nothing to see here") shouldBe "nothing to see here"
    }

    "match identically at every chunk boundary (sizes 1..N)" in {
      val input    = ByteString("x internal.example.com y internal.example.com z internal")
      val expected = oneShot(rw, input.utf8String)
      (1 to input.length).foreach(c => stream(rw, input, c) shouldBe expected)
    }

    "handle a match straddling the final boundary" in {
      val input = ByteString("prefix internal.example.com")
      (1 to input.length).foreach(c => stream(rw, input, c) shouldBe "prefix www.example.com")
    }

    "agree with LiteralRewriter (Aho-Corasick) on the same single pattern, at every split" in {
      val ac    = new LiteralRewriter(Seq("internal.example.com" -> "www.example.com"))
      val input = ByteString("p internal.example.com q internal.example.com r internal.exampl")
      (1 to input.length).foreach(c => stream(rw, input, c) shouldBe stream(ac, input, c))
    }

    "handle a single-byte pattern (degenerates to linear scan)" in {
      oneShot(BmhRewriter("x", "Y"), "axbxc") shouldBe "aYbYc"
    }

    "match non-ASCII (UTF-8 multi-byte) patterns, even split mid-character" in {
      val jp = BmhRewriter("アパッチ", "APACHE")
      oneShot(jp, "これはアパッチです、アパッチ！") shouldBe "これはAPACHEです、APACHE！"
      oneShot(BmhRewriter("Pekko", "ペッコ"), "Apache Pekko rocks") shouldBe "Apache ペッコ rocks"
      // every byte-split (including inside a 3-byte char) must still match
      val input    = ByteString("x アパッチ y アパッチ z")
      val expected = oneShot(jp, input.utf8String)
      (1 to input.length).foreach(c => stream(jp, input, c) shouldBe expected)
    }
  }
}
