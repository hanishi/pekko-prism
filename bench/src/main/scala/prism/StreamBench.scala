package prism

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.stream.scaladsl.{Flow, Sink, Source}
import org.apache.pekko.util.ByteString
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit
import scala.compiletime.uninitialized
import scala.concurrent.Await
import scala.concurrent.duration.*

/**
 * JMH benchmark for the end-to-end Pekko Streams path (`RewriteFlow`, the HTML
 * tokenizer). Compared against [[RewriteBench]] it shows the framework overhead,
 * which is near zero. One op materializes and runs the whole body through the flow.
 *
 * Run: sbt "bench/Jmh/run .*StreamBench.*"
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class StreamBench {

  private val unit =
    """<p>The head office for the body section. Visit """ +
    """<a href="http://internal.example.com/p">internal link</a> here.</p>""" + "\n"

  implicit var system: ActorSystem[Nothing]      = uninitialized
  var chunks: Vector[ByteString]                 = uninitialized
  var litFlow: Flow[ByteString, ByteString, ?]   = uninitialized
  var tokFlow: Flow[ByteString, ByteString, ?]   = uninitialized

  @Setup(Level.Trial)
  def setup(): Unit = {
    system = ActorSystem(Behaviors.empty, "jmh-stream")
    val reps = (1 * 1024 * 1024) / unit.length
    chunks  = ByteString(unit * reps).grouped(8192).toVector
    litFlow = RewriteFlow(new LiteralRewriter(Seq("internal.example.com" -> "localhost")))
    tokFlow = HtmlTextRewriteFlow(new LiteralRewriter(Seq("head" -> "HEAD")))
  }

  @TearDown(Level.Trial)
  def teardown(): Unit = {
    system.terminate()
    Await.result(system.whenTerminated, 10.seconds)
  }

  private def run(flow: Flow[ByteString, ByteString, ?]): Long =
    Await.result(Source(chunks).via(flow).runWith(Sink.fold(0L)((a, b) => a + b.length)), 30.seconds)

  @Benchmark def literalFlow(): Long   = run(litFlow)
  @Benchmark def htmlTokenizer(): Long = run(tokFlow)
}
