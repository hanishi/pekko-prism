package prism

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.apache.pekko.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.pekko.util.ByteString

/**
 * A `Flow[ByteString, ByteString]` that applies a [[Rewriter]] across a stream,
 * correctly handling matches that straddle chunk boundaries.
 *
 * The only state is `carry`: bytes received but not yet safe to emit because
 * they might be the start of a match that completes in a later chunk. On each
 * push we run the rewriter over `carry ++ chunk`, emit what it finalized, and
 * keep the rest. Backpressure is inherited from the stream — there is no manual
 * flow control. Memory is bounded by the rewriter's carry (≈ max pattern length).
 */
final class RewriteStage(rewriter: Rewriter)
    extends GraphStage[FlowShape[ByteString, ByteString]] {

  private val in  = Inlet[ByteString]("RewriteStage.in")
  private val out = Outlet[ByteString]("RewriteStage.out")
  override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

  override def createLogic(attrs: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var carry = ByteString.empty

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          carry ++= grab(in)
          val (output, consumed) = rewriter(carry, atEOF = false)
          carry = carry.drop(consumed)
          if (output.nonEmpty) push(out, output) // satisfy the outstanding demand
          else pull(in)                          // nothing ready yet → ask for more
        }

        override def onUpstreamFinish(): Unit = {
          val (output, _) = rewriter(carry, atEOF = true) // resolve partials as literal
          carry = ByteString.empty
          if (output.nonEmpty) emit(out, output, () => completeStage())
          else completeStage()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
}

object RewriteFlow {
  /** Lift a [[Rewriter]] into a reusable stream stage. */
  def apply(rewriter: Rewriter): Flow[ByteString, ByteString, NotUsed] =
    Flow.fromGraph(new RewriteStage(rewriter))
}
