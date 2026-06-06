package prism

import org.apache.pekko.util.ByteString

import java.nio.charset.{Charset, StandardCharsets}

/**
 * Streaming attribute-value rewriter — the modern, cleaned-up successor to the
 * old `prism` `QuotedSearchPattern`.
 *
 * It finds attribute anchors (`href`, `src`, …) with [[AhoCorasick]], then does
 * a bounded local parse of `name [ws] = [ws] value` and rewrites only the value
 * text via [[transform]]. Surrounding structure (the name, `=`, quotes,
 * delimiters) is preserved byte-for-byte.
 *
 * Improvements over the original:
 *   - handles double-quoted, single-quoted, AND unquoted values
 *   - tolerates whitespace around `=` (`href = "…"`)
 *   - the lookahead budget ([[maxValueLength]]) is configurable, not a hard 254
 *   - on EOF or budget overflow it emits the anchor verbatim instead of
 *     translating a truncated value (no corruption)
 *   - simple next-byte quote detection instead of the old nested byte loops
 *
 * Streaming contract: when a value is not yet fully present (no closing quote /
 * delimiter in the buffer) and we are not at EOF and still under budget, the
 * anchor and everything after it are left unconsumed (carry), to be retried when
 * more bytes arrive — the direct analogue of the original returning `null`.
 * Carry is bounded by [[maxValueLength]].
 *
 * Scope (honest): this is a heuristic, not an HTML parser. It will not look
 * inside `<script>`/`<style>`/comments for context (front it with a streaming
 * HTML tokenizer if you need that). It *does* decode HTML entities in the value
 * (`decodeEntities`) and can match anchor names case-insensitively
 * (`caseInsensitive`).
 *
 * @param anchors         attribute names that carry URLs (e.g. `Seq("href","src")`)
 * @param transform       value text in, replacement text out (identity == no change)
 * @param charset         charset for decoding/encoding the value
 * @param maxValueLength  max bytes scanned after an anchor before giving up
 * @param caseInsensitive match anchor names ignoring ASCII case (`HREF` == `href`)
 * @param decodeEntities  decode HTML character references in the value before
 *                        [[transform]] sees it (so `&amp;` reads as `&`)
 */
final class UrlAttributeRewriter(
    anchors: Seq[String] = UrlAttributeRewriter.DefaultAnchors,
    transform: String => String,
    charset: Charset = StandardCharsets.UTF_8,
    maxValueLength: Int = 8192,
    caseInsensitive: Boolean = false,
    decodeEntities: Boolean = false
) extends Rewriter {

  require(anchors.nonEmpty, "at least one anchor required")

  // When case-insensitive we drive the automaton over a lowercased view of both
  // the patterns and the input; the surrounding bytes are still emitted verbatim.
  private val ac =
    AhoCorasick(anchors.map(a => (if (caseInsensitive) a.toLowerCase else a).getBytes(charset)))

  private def fold(b: Byte): Byte =
    if (caseInsensitive && b >= 'A' && b <= 'Z') (b + 32).toByte else b

  import UrlAttributeRewriter.*

  def apply(input: ByteString, atEOF: Boolean): (ByteString, Int) = {
    if (input.isEmpty) return (ByteString.empty, 0)

    val bytes = input.toArray
    val len   = bytes.length
    val out   = ByteString.newBuilder

    var state    = ac.root
    var lastEmit = 0
    var i        = 0

    inline def emit(from: Int, until: Int): Unit =
      if (until > from) out ++= ByteString.fromArray(bytes, from, until - from)

    while (i < len) {
      state = ac.step(state, fold(bytes(i)))
      val mlen = ac.matchLenAt(state)
      if (mlen > 0) {
        val anchorStart = i - mlen + 1
        if (anchorStart >= lastEmit && wordBoundaryOk(bytes, anchorStart)) {
          parseValue(bytes, i + 1, anchorStart, len, atEOF, maxValueLength) match {
            case Parse.Complete(valueStart, valueEnd) =>
              emit(lastEmit, valueStart) // anchor, =, quote: verbatim
              val raw      = new String(bytes, valueStart, valueEnd - valueStart, charset)
              val logical  = if (decodeEntities) HtmlEntities.decode(raw) else raw
              val replaced = transform(logical)
              // If transform made no change, emit the original bytes (preserving the
              // source's own entity encoding); only decoded form leaks out on a change.
              val emitted  = if (replaced == logical) raw else replaced
              out ++= ByteString(emitted.getBytes(charset))
              lastEmit = valueEnd        // closing quote / delimiter stays literal
              i = valueEnd - 1
              state = ac.root
            case Parse.NeedMore =>
              emit(lastEmit, anchorStart) // hold the anchor + value for next chunk
              return (out.result(), anchorStart)
            case Parse.NotAttr | Parse.GiveUp =>
              state = ac.root // leave verbatim, keep scanning past the anchor
          }
        }
      }
      i += 1
    }

    if (atEOF) {
      emit(lastEmit, len)
      (out.result(), len)
    } else {
      // Hold back a possible partial anchor at the tail (AC prefix depth).
      val safe = math.max(lastEmit, len - ac.depthAt(state))
      emit(lastEmit, safe)
      (out.result(), safe)
    }
  }
}

object UrlAttributeRewriter {

  /** Common URL-bearing HTML attributes. */
  val DefaultAnchors: Seq[String] =
    Seq("href", "src", "action", "formaction", "poster", "cite")

  /**
   * Convenience: rewrite occurrences of `from` to `to` inside matched URLs.
   * Matches anchors case-insensitively and decodes HTML entities in the value,
   * which is almost always what you want for real-world HTML.
   */
  def replacingHost(
      from: String,
      to: String,
      anchors: Seq[String] = DefaultAnchors
  ): UrlAttributeRewriter =
    new UrlAttributeRewriter(
      anchors,
      transform = _.replace(from, to),
      caseInsensitive = true,
      decodeEntities = true
    )

  private enum Parse:
    case Complete(valueStart: Int, valueEnd: Int)
    case NeedMore
    case NotAttr
    case GiveUp

  private def isSpace(b: Byte): Boolean =
    b == ' ' || b == '\t' || b == '\n' || b == '\r' || b == '\f'

  /** An anchor must start at a name boundary, so `data-href` / `xhref` don't match. */
  private def wordBoundaryOk(bytes: Array[Byte], anchorStart: Int): Boolean = {
    if (anchorStart == 0) return true // boundary lived in an already-emitted chunk
    val prev = bytes(anchorStart - 1)
    isSpace(prev) || prev == '<' || prev == '/' || prev == '"' || prev == '\''
  }

  /**
   * Parse `[ws] = [ws] value` starting at index `p`. Returns the value's byte
   * range, or a signal (need more / not an attribute / over budget).
   */
  private def parseValue(
      bytes: Array[Byte],
      p: Int,
      anchorStart: Int,
      len: Int,
      atEOF: Boolean,
      budget: Int
  ): Parse = {
    val maxIdx = anchorStart + budget // scanning past here means "give up"
    var j = p

    inline def overBudget(k: Int): Boolean = k - anchorStart > budget

    // skip whitespace before '='
    while (j < len && isSpace(bytes(j))) { if (overBudget(j)) return Parse.GiveUp; j += 1 }
    if (j >= len) return if (atEOF) Parse.NotAttr else Parse.NeedMore
    if (bytes(j) != '=') return Parse.NotAttr
    j += 1
    // skip whitespace after '='
    while (j < len && isSpace(bytes(j))) { if (overBudget(j)) return Parse.GiveUp; j += 1 }
    if (j >= len) return if (atEOF) Parse.NotAttr else Parse.NeedMore

    val c = bytes(j)
    if (c == '"' || c == '\'') {
      val valueStart = j + 1
      var k = valueStart
      while (k < len) {
        if (bytes(k) == c) return Parse.Complete(valueStart, k) // closing quote found
        if (overBudget(k)) return Parse.GiveUp
        k += 1
      }
      if (atEOF) Parse.NotAttr else Parse.NeedMore // unterminated quote
    } else {
      // unquoted value: runs until whitespace or '>'
      val valueStart = j
      var k = valueStart
      while (k < len) {
        val b = bytes(k)
        if (isSpace(b) || b == '>') return Parse.Complete(valueStart, k)
        if (overBudget(k)) return Parse.GiveUp
        k += 1
      }
      if (atEOF) Parse.Complete(valueStart, len) else Parse.NeedMore
    }
  }
}
