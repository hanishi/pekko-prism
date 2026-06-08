package prism

import org.apache.pekko.util.ByteString
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class WuManberRewriterSpec extends AnyWordSpec with Matchers {

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

  "WuManberRewriter" should {
    val rw = new WuManberRewriter(Seq("internal.example.com" -> "X", "</head>" -> "Y"))

    "replace multiple patterns in one pass" in {
      oneShot(rw, "a internal.example.com b </head> c internal.example.com") shouldBe "a X b Y c X"
    }

    "leave non-matching input untouched" in {
      oneShot(rw, "nothing here at all") shouldBe "nothing here at all"
    }

    "be chunk-independent: every split (1..N) equals the single-pass result" in {
      val input    = ByteString("p internal.example.com q </head> r internal.exa</head")
      val expected = oneShot(rw, input.utf8String)
      (1 to input.length).foreach(c => stream(rw, input, c) shouldBe expected)
    }

    "agree with LiteralRewriter (Aho-Corasick) on independent patterns, at every split" in {
      val ac    = new LiteralRewriter(Seq("internal.example.com" -> "X", "</head>" -> "Y", "body" -> "B"))
      val wm    = new WuManberRewriter(Seq("internal.example.com" -> "X", "</head>" -> "Y", "body" -> "B"))
      val input = ByteString("the body has internal.example.com and </head> and body again, internal.")
      (1 to input.length).foreach(c => stream(wm, input, c) shouldBe stream(ac, input, c))
    }

    "pick the longest pattern at a start (longest-match-wins)" in {
      oneShot(new WuManberRewriter(Seq("ab" -> "1", "abcd" -> "2")), "Xabcd Yab") shouldBe "X2 Y1"
    }

    "handle non-ASCII (UTF-8) patterns at every split" in {
      val jp       = new WuManberRewriter(Seq("アパッチ" -> "A", "ペッコ" -> "P"))
      val input    = ByteString("これはアパッチとペッコです、アパッチ")
      val expected = oneShot(jp, input.utf8String)
      (1 to input.length).foreach(c => stream(jp, input, c) shouldBe expected)
    }
  }
}
