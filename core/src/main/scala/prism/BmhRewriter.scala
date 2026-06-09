package prism

import org.apache.pekko.util.ByteString

import java.nio.charset.{Charset, StandardCharsets}

/**
 * Streaming single-pattern find-and-replace using Boyer-Moore-Horspool.
 *
 * Where [[LiteralRewriter]] (Aho-Corasick) reads every byte once, BMH uses a
 * bad-character skip table to jump ahead, so on a long, sparse pattern it examines
 * far fewer than `n` bytes (sublinear on average). It is single-pattern only; for
 * a set of patterns use [[LiteralRewriter]].
 *
 * Streaming contract is the same as [[Rewriter]]: the carry never exceeds
 * `pattern.length - 1` bytes (the tail that could still complete a match next chunk),
 * so memory is bounded regardless of body size.
 */
final class BmhRewriter(pattern: Array[Byte], replacement: ByteString) extends Rewriter {

  require(pattern.nonEmpty, "pattern must be non-empty")

  private val m    = pattern.length
  private val last = m - 1

  // Horspool bad-character table: how far to shift when the byte aligned with the
  // pattern's last position is `c`. Default is m; for each pattern byte before the
  // last, the rightmost occurrence wins (ascending fill, later writes overwrite).
  private val skip = Array.fill(256)(m)
  locally {
    var k = 0
    while (k < last) { skip(pattern(k) & 0xff) = last - k; k += 1 }
  }

  /** Upper bound on bytes the caller may need to hold as carry. */
  def maxCarry: Int = m - 1

  def apply(input: ByteString, atEOF: Boolean): (ByteString, Int) = {
    if (input.isEmpty) return (ByteString.empty, 0)

    val bytes = input.toArray
    val len   = bytes.length
    val out   = ByteString.newBuilder

    var lastEmit = 0
    var i        = 0
    while (i + m <= len) {
      var j = last
      while (j >= 0 && pattern(j) == bytes(i + j)) j -= 1
      if (j < 0) { // full match at i
        if (i > lastEmit) out ++= ByteString.fromArray(bytes, lastEmit, i - lastEmit)
        out ++= replacement
        lastEmit = i + m
        i += m // non-overlapping
      } else {
        i += skip(bytes(i + last) & 0xff)
      }
    }

    // At EOF consume everything; otherwise keep the last (m-1) bytes as carry since a
    // match could still start there and complete in the next chunk.
    val end = if (atEOF) len else math.max(lastEmit, len - last)

    if (lastEmit == 0) (input.take(end), end) // no match: zero-copy slice
    else {
      if (lastEmit < end) out ++= ByteString.fromArray(bytes, lastEmit, end - lastEmit)
      (out.result(), end)
    }
  }
}

object BmhRewriter {
  def apply(from: String, to: String, charset: Charset = StandardCharsets.UTF_8): BmhRewriter =
    new BmhRewriter(from.getBytes(charset), ByteString(to.getBytes(charset)))
}
