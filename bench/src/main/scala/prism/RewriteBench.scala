package prism

import org.apache.pekko.util.ByteString
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit
import scala.compiletime.uninitialized

/**
 * JMH throughput benchmark for the rewriting engine itself (no Pekko Streams), so
 * the numbers reflect the automaton + ByteString work and nothing else. Forked JVMs
 * and proper warmup make these robust to JIT noise and resilient to background load.
 *
 * Run all:        sbt "bench/Jmh/run"
 * Run quickly:    sbt "bench/Jmh/run -f1 -wi 3 -i 3 -r 1 .*RewriteBench.*"
 *
 * AverageTime in microseconds per op; one op processes the whole body once. Divide
 * `bodyBytes` by the reported µs to get MB/s.
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
class RewriteBench {

  @Param(Array("8192"))
  var chunkSize: Int = uninitialized

  /** ~1 MB body with a moderate match density. */
  private val unit =
    """<p>The head office for the body section. Visit """ +
    """<a href="http://internal.example.com/p">internal link</a> here.</p>""" + "\n"

  var chunks: Vector[ByteString] = uninitialized
  var literal: Rewriter          = uninitialized
  var word: Rewriter             = uninitialized
  var token: Rewriter            = uninitialized

  @Setup
  def setup(): Unit = {
    val reps = (1 * 1024 * 1024) / unit.length
    val body = ByteString(unit * reps)
    chunks = body.grouped(chunkSize).toVector
    literal = new LiteralRewriter(Seq("internal.example.com" -> "localhost", "</head>" -> "x"))
    word    = new WordLiteralRewriter(Seq("head" -> "HEAD", "body" -> "BODY"))
    token   = TokenRewriter.wrappingUrls("http://internal.example.com", "http://fp/c?d={enc}")
  }

  /** Drive a Rewriter over the chunks exactly as RewriteStage does (carry + flush). */
  private def drive(rw: Rewriter): Long = {
    var carry = ByteString.empty
    var sink  = 0L
    var i     = 0
    while (i < chunks.length) {
      carry = carry ++ chunks(i)
      val (out, consumed) = rw(carry, false)
      carry = carry.drop(consumed)
      sink += out.length
      i += 1
    }
    val (out, _) = rw(carry, true)
    sink + out.length // returned so JMH consumes it (no dead-code elimination)
  }

  @Benchmark def literalRewriter(): Long = drive(literal)
  @Benchmark def wordRewriter(): Long    = drive(word)
  @Benchmark def tokenRewriter(): Long   = drive(token)
}
