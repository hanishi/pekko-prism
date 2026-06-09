package prism

import org.apache.pekko.util.ByteString

import java.nio.charset.{Charset, StandardCharsets}

/**
 * Streaming multi-pattern find-and-replace using Wu-Manber: Boyer-Moore-style skipping
 * generalized to a set of patterns. A bad-shift table keyed by 2-byte blocks lets the
 * scan jump ahead, so on a set of long-ish patterns it beats Aho-Corasick's every-byte
 * walk.
 *
 * The skip distance is bounded by the SHORTEST pattern's length (only the first `m`
 * bytes of each pattern drive the shift table), so with one short pattern the skip
 * collapses toward linear. Requires every pattern to be at least 2 bytes (block size).
 *
 * Semantics match [[LiteralRewriter]]: non-overlapping, leftmost; at a given start the
 * longest pattern wins. Carry never exceeds `maxPatternLength - 1` bytes.
 */
final class WuManberRewriter(
    replacements: Seq[(String, String)],
    charset: Charset = StandardCharsets.UTF_8
) extends Rewriter {

  require(replacements.nonEmpty, "at least one replacement required")

  private val patterns = replacements.map(_._1.getBytes(charset)).toArray
  private val repls     = replacements.map(p => ByteString(p._2.getBytes(charset))).toArray
  private val r         = patterns.length
  private val m         = patterns.map(_.length).min // shortest pattern = scan window
  private val maxLen    = patterns.map(_.length).max
  require(m >= 2, "Wu-Manber (block size 2) needs every pattern to be at least 2 bytes")

  private val B    = 2
  private val SIZE = 1 << 16 // exact 2-byte block index, no hash collisions
  private val shift = Array.fill(SIZE)(m - B + 1) // default: maximal safe shift
  private val cand  = Array.fill(SIZE)(List.empty[Int]) // block -> patterns whose window ends with it

  locally {
    var p = 0
    while (p < r) {
      val pat = patterns(p)
      var q = 0
      while (q <= m - B) {
        val h = (pat(q) & 0xff) << 8 | (pat(q + 1) & 0xff)
        val s = m - B - q
        if (s < shift(h)) shift(h) = s
        if (s == 0) cand(h) = p :: cand(h) // last block of this pattern's first-m window
        q += 1
      }
      p += 1
    }
  }

  /** Upper bound on bytes the caller may need to hold as carry. */
  def maxCarry: Int = maxLen - 1

  private def matchesAt(bytes: Array[Byte], start: Int, pat: Array[Byte]): Boolean = {
    var k = 0
    while (k < pat.length) { if (bytes(start + k) != pat(k)) return false; k += 1 }
    true
  }

  def apply(input: ByteString, atEOF: Boolean): (ByteString, Int) = {
    if (input.isEmpty) return (ByteString.empty, 0)
    val bytes = input.toArray
    val len   = bytes.length
    if (len < m) return if (atEOF) (input, len) else (ByteString.empty, 0)

    val out = ByteString.newBuilder
    var lastEmit = 0
    // A match at start s is only committed once every pattern that could start there fits
    // in the buffer (s + maxLen <= len), so a longer pattern can never complete next chunk
    // and steal the position. Past that limit we stop and carry.
    val commitLimit = if (atEOF) len - m + 1 else len - maxLen + 1
    var i = m - 1 // window end; the m-window covers [i-m+1, i]
    while (i < len) {
      val h = (bytes(i - 1) & 0xff) << 8 | (bytes(i) & 0xff)
      val s = shift(h)
      if (s > 0) i += s
      else {
        val start = i - m + 1
        if (start >= commitLimit) i = len // unsafe tail: stop, the rest is carry
        else if (start < lastEmit) i += 1 // overlaps an emitted replacement
        else {
          var bestLen = -1
          var bestId  = -1
          var cs      = cand(h)
          while (cs.nonEmpty) {
            val pid = cs.head
            val pat = patterns(pid)
            if (start + pat.length <= len && pat.length > bestLen && matchesAt(bytes, start, pat)) {
              bestLen = pat.length; bestId = pid
            }
            cs = cs.tail
          }
          if (bestId >= 0) {
            if (start > lastEmit) out ++= ByteString.fromArray(bytes, lastEmit, start - lastEmit)
            out ++= repls(bestId)
            lastEmit = start + bestLen
            i = lastEmit + m - 1
          } else i += 1
        }
      }
    }

    val end = if (atEOF) len else math.max(lastEmit, len - maxLen + 1)
    if (lastEmit == 0) (input.take(end), end)
    else {
      if (lastEmit < end) out ++= ByteString.fromArray(bytes, lastEmit, end - lastEmit)
      (out.result(), end)
    }
  }
}
