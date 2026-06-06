package prism

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

class WordLiteralRewriterSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("word-rewriter-spec")
  override def afterAll(): Unit = Await.result(system.terminate(), 5.seconds)

  private val rw = new WordLiteralRewriter(Seq("head" -> "HEAD", "rate" -> "RATE"))

  private def oneShot(s: String): String = rw(ByteString(s), atEOF = true)._1.utf8String

  private def stream(chunks: Seq[ByteString]): String =
    Await.result(
      Source(chunks.toList).via(RewriteFlow(rw)).runFold(ByteString.empty)(_ ++ _),
      5.seconds
    ).utf8String

  "WordLiteralRewriter (single-shot)" should {
    "replace a standalone word" in {
      oneShot("go to head now") shouldBe "go to HEAD now"
    }
    "not replace a substring of a larger word" in {
      oneShot("the header is ahead")     shouldBe "the header is ahead"
      oneShot("headache and forehead")   shouldBe "headache and forehead"
    }
    "treat punctuation as a boundary" in {
      oneShot("(head), head. head!") shouldBe "(HEAD), HEAD. HEAD!"
    }
    "replace a word at the very start and end of input" in {
      oneShot("head")          shouldBe "HEAD"
      oneShot("head and rate") shouldBe "HEAD and RATE"
      oneShot("a rate")        shouldBe "a RATE"
    }
    "not match adjacent word characters (digits, underscore)" in {
      oneShot("head1 _head head_") shouldBe "head1 _head head_"
    }
    "be honest: markup is NOT protected (that is the tokenizer's job)" in {
      // '<' and '>' are boundaries, so the word inside the tag still matches.
      oneShot("<head>") shouldBe "<HEAD>"
    }
  }

  "WordLiteralRewriter (streaming)" should {
    // Adversarial: words split mid-pattern, preceded/followed by word chars.
    val doc =
      "head, header, ahead, head; the rate of headache vs rate. head_ _rate head"
    val bytes    = ByteString(doc)
    val expected = oneShot(doc)

    "match the single-shot result for ANY chunk boundary" in {
      for (size <- 1 to bytes.length) withClue(s"chunk size = $size: ") {
        stream(bytes.grouped(size).toList) shouldBe expected
      }
    }

    "decide the right boundary when it arrives in a later chunk" in {
      // "head" then "er" → header, must NOT replace
      stream(List(ByteString("see head"), ByteString("er here"))) shouldBe "see header here"
      // "head" then space → whole word, MUST replace
      stream(List(ByteString("see head"), ByteString(" here"))) shouldBe "see HEAD here"
    }

    "decide the left boundary when a match starts a later chunk" in {
      // 'a' + 'head' → ahead, must NOT replace even though 'head' starts the chunk
      stream(List(ByteString("go a"), ByteString("head now"))) shouldBe "go ahead now"
    }
  }
}
