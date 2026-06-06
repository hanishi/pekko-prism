package prism

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.ByteString

import scala.concurrent.Await
import scala.concurrent.duration.*

/**
 * Throughput micro-benchmark for the rewriting engine. Not JMH-rigorous (no
 * forking/blackholes), but warmed up and min-of-N, so the numbers are a fair
 * single-core ballpark. Run: `runMain prism.Bench`.
 */
object Bench {

  def main(args: Array[String]): Unit = {
    // ~16 MB of realistic HTML with a moderate match density.
    val unit =
      """<p>The head office content for the body section. Visit """ +
      """<a href="http://internal.example.com/page">internal link</a> now, """ +
      """and read a little more text here to pad the line out.</p>""" + "\n"
    val reps  = (16 * 1024 * 1024) / unit.length
    val body  = ByteString(unit * reps)
    val total = body.length.toLong
    val chunkSize = 8192
    val chunks = body.grouped(chunkSize).toVector

    def report(name: String, nanos: Long): Unit = {
      val mbps = total.toDouble / (nanos / 1e9) / (1024 * 1024)
      println(f"  $name%-30s ${mbps}%8.0f MB/s   ${nanos.toDouble / total}%5.2f ns/byte")
    }

    println(f"body=${total / (1024 * 1024)}%d MB, chunk=$chunkSize B, chunks=${chunks.size}%d")
    println("--- pure engine (Rewriter.apply, no Pekko Streams) ---")

    // Drive a Rewriter exactly as RewriteStage does, but with no stream overhead.
    def pure(name: String, rw: Rewriter): Unit = {
      def once(): Long = {
        val t0 = System.nanoTime()
        var carry = ByteString.empty
        var sink  = 0L
        var i = 0
        while (i < chunks.length) {
          carry = carry ++ chunks(i)
          val (out, consumed) = rw(carry, false)
          carry = carry.drop(consumed)
          sink += out.length
          i += 1
        }
        val (out, _) = rw(carry, true)
        sink += out.length
        if (sink == Long.MinValue) println(sink) // defeat DCE
        System.nanoTime() - t0
      }
      (1 to 5).foreach(_ => once())
      val best = (1 to 12).map(_ => once()).min
      report(name, best)
    }

    pure("passthrough (no-op)", new Rewriter { def apply(in: ByteString, eof: Boolean) = (in, in.length) })
    pure("LiteralRewriter",     new LiteralRewriter(Seq("internal.example.com" -> "localhost", "</head>" -> "x")))
    pure("WordLiteralRewriter", new WordLiteralRewriter(Seq("head" -> "HEAD", "body" -> "BODY")))
    pure("TokenRewriter (wrap)", TokenRewriter.wrappingUrls("http://internal.example.com", "http://fp/c?d={enc}"))

    println("--- end-to-end through Pekko Streams (Flow) ---")
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "bench")
    try {
      def streamed(name: String, flow: Flow[ByteString, ByteString, ?]): Unit = {
        def once(): Long = {
          val t0 = System.nanoTime()
          Await.result(Source(chunks).via(flow).runWith(Sink.fold(0L)((a, b) => a + b.length)), 60.seconds)
          System.nanoTime() - t0
        }
        (1 to 5).foreach(_ => once())
        report(name, (1 to 12).map(_ => once()).min)
      }
      streamed("passthrough (Flow)",       Flow[ByteString])
      streamed("LiteralRewriter (Flow)",   RewriteFlow(new LiteralRewriter(Seq("internal.example.com" -> "localhost"))))
      streamed("HtmlTextRewrite tokenizer", HtmlTextRewriteFlow(new LiteralRewriter(Seq("head" -> "HEAD"))))
    } finally system.terminate()
  }
}
