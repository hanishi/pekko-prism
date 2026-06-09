package prism

import org.apache.pekko.util.ByteString

import java.nio.charset.{Charset, StandardCharsets}

/**
 * A stateful, chunk-boundary-aware byte rewriter.
 *
 * Given the currently buffered `input`, produce the bytes that are safe to emit
 * now and report how many leading bytes of `input` were *finalized*. Any
 * trailing bytes that could still grow into a match must be left unconsumed —
 * the caller keeps them as the "carry" and prepends them to the next chunk.
 *
 * This is the direct analogue of `prism`'s `ContentFilter` (`replace()==null`
 * meaning "need more bytes" is just a smaller `consumed`) and of Go's
 * `transform.Transformer` (`ErrShortSrc` / `nSrc`).
 *
 * @param atEOF true on the final call: no more bytes will ever arrive, so any
 *              dangling partial match must resolve to literal output and the
 *              whole input must be consumed.
 * @return (output, consumed) where 0 <= consumed <= input.length
 */
trait Rewriter {
  def apply(input: ByteString, atEOF: Boolean): (ByteString, Int)
}

/**
 * Streaming multi-pattern literal find-and-replace.
 *
 * Semantics: non-overlapping, scanning left to right. When several patterns end
 * at the same position the longest one is replaced; the scan then resumes after
 * the replacement (replacements never re-match each other). Patterns that are
 * proper prefixes of other patterns may therefore match the shorter form — for
 * the typical anchors (`href="`, a domain, `</head>`) no pattern prefixes
 * another, so the result is exactly leftmost-longest.
 *
 * The carry retained between calls never exceeds `maxPatternLength - 1` bytes,
 * so memory is bounded regardless of body size.
 */
final class LiteralRewriter(
    replacements: Seq[(String, String)],
    charset: Charset = StandardCharsets.UTF_8
) extends Rewriter {

  require(replacements.nonEmpty, "at least one replacement required")

  private val patterns = replacements.map(_._1.getBytes(charset)).toVector
  private val repls     = replacements.map(p => ByteString(p._2.getBytes(charset))).toArray
  private val ac        = AhoCorasick(patterns.map(identity))

  /** Upper bound on bytes the caller may need to hold as carry. */
  def maxCarry: Int = math.max(0, ac.maxPatternLength - 1)

  def apply(input: ByteString, atEOF: Boolean): (ByteString, Int) = {
    if (input.isEmpty) return (ByteString.empty, 0)

    val bytes = input.toArray
    val out   = ByteString.newBuilder

    var state    = ac.root
    var lastEmit = 0 // next input index not yet written to `out`
    var i        = 0

    while (i < bytes.length) {
      state = ac.step(state, bytes(i))
      val mlen = ac.matchLenAt(state)
      if (mlen > 0) {
        val start = i - mlen + 1
        if (start >= lastEmit) { // non-overlapping with what we already emitted
          if (start > lastEmit) out ++= ByteString.fromArray(bytes, lastEmit, start - lastEmit)
          out ++= repls(ac.matchIdAt(state))
          lastEmit = i + 1
          state = ac.root // resume fresh after the replacement
        }
      }
      i += 1
    }

    // Final consumed point: at EOF everything; otherwise everything that can no longer
    // start a future match (len - currentDepth), computed once, not per byte.
    val end =
      if (atEOF) bytes.length
      else math.max(lastEmit, bytes.length - ac.depthAt(state))

    // Fast path: no replacement happened (lastEmit still 0), so the output is just the
    // unchanged prefix — emit it as a zero-copy slice of the input rather than copying
    // it through the builder. This is the common case for sparse patterns.
    if (lastEmit == 0) (input.take(end), end)
    else {
      if (lastEmit < end) out ++= ByteString.fromArray(bytes, lastEmit, end - lastEmit)
      (out.result(), end)
    }
  }
}
