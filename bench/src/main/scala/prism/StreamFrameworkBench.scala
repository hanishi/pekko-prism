package prism

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{Sink, Source}
import org.apache.pekko.util.ByteString
import org.openjdk.jmh.annotations.*
import zio.{Chunk, Runtime, Unsafe}
import zio.stream.ZStream

import java.util.concurrent.TimeUnit
import scala.compiletime.uninitialized
import scala.concurrent.Await
import scala.concurrent.duration.*

/**
 * Same engine, same body, two streaming runtimes: Pekko Streams (`RewriteFlow`, a
 * `GraphStage`) vs ZIO Streams (`ZioRewrite.pipeline`, a `ZChannel`). Isolates the
 * framework overhead, since the rewriter work is identical.
 *
 * Caveat: the ZIO path converts `Chunk[Byte]` <-> `ByteString` per chunk (the core engine
 * is ByteString-based); the Pekko path does not. A Chunk-native core would remove that.
 *
 *   sbt "bench/Jmh/run -f1 -wi 4 -i 5 .*StreamFrameworkBench.*"
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 4, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(1)
class StreamFrameworkBench {

  @Param(Array("8192"))
  var chunkSize: Int = uninitialized

  private val unit =
    """<p>The head office for the body section. Visit """ +
    """<a href="http://internal.example.com/p">internal link</a> here.</p></head>""" + "\n"

  var pekkoChunks: Vector[ByteString] = uninitialized
  var zioChunks: Vector[Chunk[Byte]]  = uninitialized
  var rw: Rewriter                    = uninitialized
  var total: Long                     = uninitialized
  implicit var system: ActorSystem    = uninitialized

  @Setup def setup(): Unit = {
    val reps = (1 * 1024 * 1024) / unit.length
    val body = ByteString(unit * reps)
    total       = body.length.toLong
    pekkoChunks = body.grouped(chunkSize).toVector
    zioChunks   = pekkoChunks.map(bs => Chunk.fromArray(bs.toArray))
    rw          = new WuManberRewriter(Seq("internal.example.com" -> "localhost", "</head>" -> "x"))
    system      = ActorSystem("stream-framework-bench")
  }

  @TearDown def teardown(): Unit = Await.result(system.terminate(), 10.seconds)

  @Benchmark def pekkoFlow(): Long = {
    Await.result(Source(pekkoChunks).via(RewriteFlow(rw)).runWith(Sink.ignore), 30.seconds)
    total
  }

  @Benchmark def zioPipeline(): Long = {
    Unsafe.unsafe { implicit u =>
      Runtime.default.unsafe
        .run(ZStream.fromChunks(zioChunks*).via(ZioRewrite.pipeline(rw)).runDrain)
        .getOrThrow()
    }
    total
  }
}
