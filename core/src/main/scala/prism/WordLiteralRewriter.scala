package prism

import org.apache.pekko.util.ByteString

import java.nio.charset.{Charset, StandardCharsets}

/**
 * Whole-word streaming find/replace: like [[LiteralRewriter]], but a pattern is
 * replaced only when it is bounded by a non-word character (or the stream
 * start/end) on BOTH sides. So `head -> HEAD` rewrites the word "head" but leaves
 * "header", "ahead" and "headache" alone.
 *
 * A "word character" is an ASCII letter, digit or underscore, plus any byte with
 * the high bit set (so accented / multibyte UTF-8 letters count as part of a word
 * and are never split). Everything else — space, punctuation, `<`, `>`, `=`,
 * quotes — is a boundary.
 *
 * '''Important scope.''' This is `\b`-style word matching, NOT markup awareness.
 * Because `<` and `>` are boundaries, the word "head" inside `<head>` still
 * matches (it becomes `<HEAD>`). If you need to skip tags / attributes / scripts,
 * that requires the HTML tokenizer, not this rewriter.
 *
 * Streaming correctness: deciding the RIGHT boundary needs one byte past the
 * match, and deciding the LEFT boundary of a match that lands at the very start
 * of the next chunk needs one byte of left context — so the carry retains up to
 * one byte before the held region. Carry stays bounded by `maxPatternLength + 1`.
 */
final class WordLiteralRewriter(
    replacements: Seq[(String, String)],
    charset: Charset = StandardCharsets.UTF_8
) extends Rewriter {

  require(replacements.nonEmpty, "at least one replacement required")

  private val patterns = replacements.map(_._1.getBytes(charset)).toVector
  private val repls     = replacements.map(p => ByteString(p._2.getBytes(charset))).toArray
  private val ac        = AhoCorasick(patterns.map(identity))

  /** Upper bound on bytes the caller may need to hold as carry. */
  def maxCarry: Int = ac.maxPatternLength + 1

  private def isWordByte(b: Byte): Boolean = {
    val u = b & 0xff
    (u >= 'a' && u <= 'z') || (u >= 'A' && u <= 'Z') ||
    (u >= '0' && u <= '9') || u == '_' || u >= 0x80
  }
  private def isBoundary(b: Byte): Boolean = !isWordByte(b)

  def apply(input: ByteString, atEOF: Boolean): (ByteString, Int) = {
    if (input.isEmpty) return (ByteString.empty, 0)

    val bytes = input.toArray
    val len   = bytes.length
    val out   = ByteString.newBuilder

    inline def emit(from: Int, until: Int): Unit =
      if (until > from) out ++= ByteString.fromArray(bytes, from, until - from)

    var state    = ac.root
    var lastEmit = 0
    var i        = 0

    while (i < len) {
      state = ac.step(state, bytes(i))
      val mlen = ac.matchLenAt(state)
      if (mlen > 0) {
        val start = i - mlen + 1
        if (start >= lastEmit) {
          // Left boundary: stream start counts as a boundary; otherwise look at
          // the preceding byte (always present — we retain one byte of context).
          val leftOK = start == 0 || isBoundary(bytes(start - 1))
          if (leftOK) {
            if (i + 1 < len) {
              if (isBoundary(bytes(i + 1))) {
                emit(lastEmit, start)
                out ++= repls(ac.matchIdAt(state))
                lastEmit = i + 1
                state = ac.root // resume fresh after the replacement
              }
              // else: a word char abuts the match → not a whole word; keep scanning
            } else if (atEOF) {
              // match ends exactly at the end of the final input → right boundary
              emit(lastEmit, start)
              out ++= repls(ac.matchIdAt(state))
              lastEmit = i + 1
              state = ac.root
            } else {
              // match ends at the buffer edge and more bytes may come: we can't yet
              // tell if the next byte is a boundary. Hold the match (and one byte of
              // left context) for the next call.
              val keepFrom = if (start > lastEmit) start - 1 else lastEmit
              emit(lastEmit, keepFrom)
              return (out.result(), keepFrom)
            }
          }
        }
      }
      i += 1
    }

    if (atEOF) {
      emit(lastEmit, len)
      (out.result(), len)
    } else {
      // Hold back any partial match prefix at the tail (AC depth), plus one byte of
      // left context so the next call can judge a match that starts right there.
      val safe     = math.max(lastEmit, len - ac.depthAt(state))
      val keepFrom = if (safe > lastEmit) safe - 1 else lastEmit
      emit(lastEmit, keepFrom)
      (out.result(), keepFrom)
    }
  }
}
