package prism

import org.apache.pekko.util.ByteString
import zio.Chunk
import zio.stream.{ZChannel, ZPipeline}

/**
 * Drives a [[Rewriter]] from ZIO Streams. This is the same chunk-boundary-aware engine
 * (the matchers and rewriters in `pekko-prism-core`), wrapped in a `ZPipeline` instead of
 * a Pekko `GraphStage`. No HTTP, no Pekko Streams; the only shared type is `ByteString`.
 *
 * The carry contract is exactly the one in `RewriteStage`: accumulate `carry ++ chunk`,
 * call the rewriter, emit the safe output, keep the unconsumed tail as the carry, and
 * flush with `atEOF = true` when the stream ends. The byte container is converted at the
 * boundary (`Chunk[Byte]` <-> `ByteString`); the algorithm code is untouched.
 */
object ZioRewrite {

  def pipeline(rw: Rewriter): ZPipeline[Any, Nothing, Byte, Byte] = {
    type Ch = ZChannel[Any, Nothing, Chunk[Byte], Any, Nothing, Chunk[Byte], Any]

    def emit(out: ByteString): Ch =
      if (out.isEmpty) ZChannel.succeed(())
      else ZChannel.write(Chunk.fromArray(out.toArray))

    def loop(carry: ByteString): Ch =
      ZChannel.readWith(
        (in: Chunk[Byte]) => {
          val buf             = carry ++ ByteString.fromArray(in.toArray)
          val (out, consumed) = rw(buf, atEOF = false)
          emit(out) *> loop(buf.drop(consumed))
        },
        (err: Nothing) => err,                 // input error type is Nothing: unreachable
        (_: Any) => {
          val (out, _) = rw(carry, atEOF = true)
          emit(out)
        }
      )

    ZPipeline.fromChannel(loop(ByteString.empty))
  }
}
