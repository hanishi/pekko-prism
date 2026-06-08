package prism

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import org.apache.pekko.stream.{Attributes, FlowShape, Inlet, Outlet}
import org.apache.pekko.util.{ByteString, ByteStringBuilder}

/**
 * A `Flow[ByteString, ByteString]` that applies an inner [[Rewriter]] only to the
 * '''text content''' of an HTML stream — never to tags, attributes, `<script>` /
 * `<style>` bodies, or comments. This is the context-aware mode: it makes even
 * common words safe to rewrite, because `<head>`, `class="header"` and
 * `<script>head</script>` are all left untouched while the word "head" in visible
 * text is rewritten.
 *
 * It is a streaming, single-pass tokenizer driven one chunk at a time. The parse
 * mode (text / tag / comment / raw-text) and the inner rewriter's text carry both
 * live in the stage logic, so they are per-stream and survive chunk boundaries;
 * the same patterns split across packets still match within a text run. Patterns
 * do NOT match across a tag boundary (each text run is rewritten independently),
 * which is the correct HTML semantics — `inter<b>nal` is two text nodes.
 *
 * Scope: it understands tags (with quoted attribute values that may contain `>`),
 * `<!-- comments -->`, and `<script>`/`<style>` raw-text elements (closed by
 * `</script` / `</style`). It does not special-case CDATA, processing
 * instructions, or SVG/MathML foreign content; those are emitted verbatim, which
 * is safe (it just means no rewriting inside them).
 */
final class HtmlTextRewriteStage(inner: Rewriter)
    extends GraphStage[FlowShape[ByteString, ByteString]] {

  private val in  = Inlet[ByteString]("HtmlTextRewrite.in")
  private val out = Outlet[ByteString]("HtmlTextRewrite.out")
  override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

  private final val TEXT    = 0
  private final val TAG     = 1
  private final val COMMENT = 2
  private final val RAW     = 3

  private val ScriptClose = "</script".getBytes("US-ASCII")
  private val StyleClose  = "</style".getBytes("US-ASCII")

  override def createLogic(attrs: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) {
      private var carry     = ByteString.empty // unprocessed raw bytes (markup lookahead)
      private var textCarry = ByteString.empty // inner rewriter carry for the current text run

      private var mode         = TEXT
      private var quote        = 0    // inside a tag: 0, or the open quote char code
      private var lastNonSpace = 0    // inside a tag: last non-space byte (for `/>` detection)
      private var isClosing    = false
      private var pendingRaw: Array[Byte] = Array.emptyByteArray // close target if open tag is script/style
      private var rawClose: Array[Byte]   = Array.emptyByteArray // active raw-text close target
      private var rawMatch     = 0
      private var dashCount     = 0   // inside a comment: trailing run of '-'

      private def isLetter(b: Byte): Boolean = {
        val u = b & 0xff; (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z')
      }
      private def isSpace(b: Byte): Boolean =
        b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f'
      private def lower(b: Byte): Int = {
        val u = b & 0xff; if (u >= 'A' && u <= 'Z') u + 32 else u
      }

      /** Stream text bytes through the inner rewriter (carry kept in textCarry). */
      private def feedText(b: ByteString, out: ByteStringBuilder): Unit = {
        textCarry ++= b
        val (o, consumed) = inner(textCarry, atEOF = false)
        textCarry = textCarry.drop(consumed)
        if (o.nonEmpty) out ++= o
      }

      /** End the current text run: flush the inner rewriter's remaining carry. */
      private def flushText(out: ByteStringBuilder): Unit =
        if (textCarry.nonEmpty) {
          val (o, _) = inner(textCarry, atEOF = true)
          textCarry = ByteString.empty
          if (o.nonEmpty) out ++= o
        }

      // Enter TAG mode at the current '<'. We do NOT emit or advance here: the TAG
      // branch emits verbatim starting from the '<' itself.
      private def beginTag(out: ByteStringBuilder, closing: Boolean, raw: Array[Byte]): Unit = {
        flushText(out)
        isClosing = closing; pendingRaw = raw; lastNonSpace = 0; quote = 0
        mode = TAG
      }

      /** Process as much of `carry` as is unambiguous; update state; emit into `out`. */
      private def process(atEOF: Boolean, out: ByteStringBuilder): Unit = {
        val a = carry.toArray
        val n = a.length
        var p = 0
        var hold = false

        while (p < n && !hold) {
          mode match {
            case TEXT =>
              var j = p
              while (j < n && a(j) != '<') j += 1
              if (j > p) feedText(ByteString.fromArray(a, p, j - p), out)
              p = j
              if (p < n) { // a(p) == '<'
                if (p + 1 >= n) {
                  if (atEOF) { flushText(out); out ++= ByteString.fromArray(a, p, 1); p += 1 }
                  else hold = true
                } else {
                  val c1 = a(p + 1)
                  if (c1 == '!') {
                    if (p + 3 < n) {
                      if (a(p + 2) == '-' && a(p + 3) == '-') {
                        flushText(out)
                        out ++= ByteString.fromArray(a, p, 4); p += 4; mode = COMMENT; dashCount = 0
                      } else beginTag(out, closing = false, raw = Array.emptyByteArray) // <!doctype …>
                    } else if (atEOF) beginTag(out, closing = false, raw = Array.emptyByteArray)
                    else hold = true
                  } else if (c1 == '/') {
                    beginTag(out, closing = true, raw = Array.emptyByteArray)
                  } else if (isLetter(c1)) {
                    var k = p + 1
                    while (k < n && isLetter(a(k))) k += 1
                    if (k >= n && !atEOF) hold = true
                    else {
                      val name = new String(a, p + 1, k - (p + 1)).toLowerCase
                      val raw  = if (name == "script") ScriptClose
                                 else if (name == "style") StyleClose
                                 else Array.emptyByteArray
                      beginTag(out, closing = false, raw = raw)
                    }
                  } else {
                    feedText(ByteString.fromArray(a, p, 1), out); p += 1 // '<' is literal text
                  }
                }
              }

            case TAG =>
              val segStart = p
              var done = false
              while (p < n && !done) {
                val b = a(p)
                if (quote != 0) {
                  if ((b & 0xff) == quote) quote = 0
                } else if (b == '"' || b == '\'') {
                  quote = b & 0xff
                } else if (b == '>') {
                  val toRaw = pendingRaw.nonEmpty && !isClosing && lastNonSpace != '/'
                  if (toRaw) { rawClose = pendingRaw; rawMatch = 0; mode = RAW } else mode = TEXT
                  pendingRaw = Array.emptyByteArray; isClosing = false; lastNonSpace = 0
                  done = true
                } else if (!isSpace(b)) lastNonSpace = b
                p += 1
              }
              out ++= ByteString.fromArray(a, segStart, p - segStart)

            case COMMENT =>
              val segStart = p
              while (p < n && mode == COMMENT) {
                val b = a(p)
                if (b == '-') dashCount += 1
                else if (b == '>') { if (dashCount >= 2) mode = TEXT; dashCount = 0 }
                else dashCount = 0
                p += 1
              }
              out ++= ByteString.fromArray(a, segStart, p - segStart)

            case RAW =>
              val segStart = p
              val r        = rawClose
              val r0       = lower(r(0))
              while (p < n && mode == RAW) {
                if (lower(a(p)) == r(rawMatch)) {
                  rawMatch += 1
                  if (rawMatch == r.length) {
                    // matched "</script"/"</style"; consume the rest of the tag as TAG
                    isClosing = true; pendingRaw = Array.emptyByteArray; lastNonSpace = 0; quote = 0
                    rawMatch = 0; rawClose = Array.emptyByteArray; mode = TAG
                  }
                } else rawMatch = if (lower(a(p)) == r0) 1 else 0
                p += 1
              }
              out ++= ByteString.fromArray(a, segStart, p - segStart)
          }
        }

        carry = if (p >= n) ByteString.empty else ByteString.fromArray(a, p, n - p)
      }

      setHandler(in, new InHandler {
        override def onPush(): Unit = {
          carry ++= grab(in)
          val b = ByteString.newBuilder
          process(atEOF = false, b)
          val output = b.result()
          if (output.nonEmpty) push(out, output) else pull(in)
        }

        override def onUpstreamFinish(): Unit = {
          val b = ByteString.newBuilder
          process(atEOF = true, b)
          if (carry.nonEmpty) b ++= carry // any unclassifiable tail → verbatim
          flushText(b)
          val output = b.result()
          if (output.nonEmpty) emit(out, output, () => completeStage()) else completeStage()
        }
      })

      setHandler(out, new OutHandler {
        override def onPull(): Unit = pull(in)
      })
    }
}

object HtmlTextRewriteFlow {
  /** Apply `inner` only to HTML text content (skip tags/scripts/styles/comments). */
  def apply(inner: Rewriter): Flow[ByteString, ByteString, NotUsed] =
    Flow.fromGraph(new HtmlTextRewriteStage(inner))
}
