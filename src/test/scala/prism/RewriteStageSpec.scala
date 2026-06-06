package prism

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration.*

class RewriteStageSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("rewrite-spec")

  override def afterAll(): Unit = Await.result(system.terminate(), 5.seconds)

  private val rewriter = new LiteralRewriter(
    Seq(
      "internal.example.com" -> "localhost",
      "</head>"              -> "<x/></head>"
    )
  )

  /** Run a specific chunking through the streaming flow and return the result. */
  private def runChunks(chunks: Seq[ByteString]): String = {
    val f = Source(chunks.toList)
      .via(RewriteFlow(rewriter))
      .runFold(ByteString.empty)(_ ++ _)
    Await.result(f, 5.seconds).utf8String
  }

  /** Reference result: sequential non-overlapping replacement (matches our semantics
    * because no pattern/replacement re-creates another pattern here). */
  private def reference(s: String): String =
    s.replace("internal.example.com", "localhost").replace("</head>", "<x/></head>")

  "RewriteStage" should {
    val input =
      """<head><title>t</title></head>
        |<a href="http://internal.example.com/a">x</a>
        |<img src="http://internal.example.com/b.png"/>""".stripMargin
    val bytes    = ByteString(input)
    val expected = reference(input)

    "produce the reference output for ANY chunk boundary" in {
      for (size <- 1 to bytes.length) {
        val chunks = bytes.grouped(size).toList
        withClue(s"chunk size = $size: ") {
          runChunks(chunks) shouldBe expected
        }
      }
    }

    "handle the whole body as a single chunk" in {
      runChunks(List(bytes)) shouldBe expected
    }

    "handle many tiny single-byte chunks" in {
      runChunks(bytes.map(b => ByteString(b)).toList) shouldBe expected
    }

    "emit a pattern split exactly across two chunks" in {
      // "internal.example.com" split right down the middle
      val a = ByteString("see http://internal.exa")
      val b = ByteString("mple.com/end")
      runChunks(List(a, b)) shouldBe "see http://localhost/end"
    }

    "leave a dangling partial match as literal at EOF" in {
      // stream ends mid-pattern: nothing more will arrive, so emit it verbatim
      runChunks(List(ByteString("trailing </hea"))) shouldBe "trailing </hea"
    }

    "pass through input with no matches unchanged" in {
      val s = "nothing to see here, move along"
      runChunks(s.grouped(3).map(ByteString(_)).toList) shouldBe s
    }
  }
}
