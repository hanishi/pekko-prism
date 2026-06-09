package prism

import org.apache.pekko.util.ByteString

import java.nio.charset.{Charset, StandardCharsets}

/**
 * Streaming '''capture-and-transform''' rewriter: find a token that starts at one
 * of `anchors`, capture it up to the next boundary byte, and replace it with
 * `transform(captured)`. Unlike [[LiteralRewriter]] (static `from -> to`), the
 * replacement is a function of what was actually there — which is what
 * "capture the original" needs.
 *
 * The motivating case is first-party proxying of ad/measurement URLs: anchor on
 * `https://tracker.example.com`, capture the whole URL, and emit
 * `https://fp.publisher.com/collect?dest=<original-url-encoded>` so a first-party
 * endpoint can forward the hit. See [[TokenRewriter.wrappingUrls]].
 *
 * It generalises [[UrlAttributeRewriter]]'s `transform` from HTML attribute values
 * to any context — element text, `<![CDATA[…]]>`, plain bytes — so it works on
 * VAST/XML as well as HTML.
 *
 * Streaming contract: when the token has no boundary yet in the buffer and we are
 * not at EOF and still under [[maxTokenLength]], the anchor and everything after
 * it are held as carry (the "need more" case). Carry is bounded by
 * `maxTokenLength`. On EOF an unterminated token is captured as-is; over budget it
 * is emitted verbatim rather than transformed.
 *
 * @param anchors        token start markers (e.g. `Seq("http://","https://")`)
 * @param transform      captured token in, replacement out
 * @param isBoundary     byte that ends a token (default: URL delimiters)
 * @param maxTokenLength max bytes captured after an anchor before giving up
 */
final class TokenRewriter(
    anchors: Seq[String],
    transform: String => String,
    isBoundary: Byte => Boolean = TokenRewriter.UrlBoundary,
    charset: Charset = StandardCharsets.UTF_8,
    maxTokenLength: Int = 8192
) extends Rewriter {

  require(anchors.nonEmpty, "at least one anchor required")

  private val ac = AhoCorasick(anchors.map(_.getBytes(charset)))

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

    inline def replace(start: Int, end: Int): Unit = {
      emit(lastEmit, start)
      val token = new String(bytes, start, end - start, charset)
      out ++= ByteString(transform(token).getBytes(charset))
      lastEmit = end
    }

    while (i < len) {
      state = ac.step(state, bytes(i))
      val mlen = ac.matchLenAt(state)
      if (mlen > 0) {
        val anchorStart = i - mlen + 1
        if (anchorStart >= lastEmit) {
          // capture from the anchor start until a boundary / budget / end of input
          var k   = i + 1
          var end = -1 // -1: need more, -2: over budget, >=0: boundary index
          while (k < len && end == -1) {
            if (isBoundary(bytes(k))) end = k
            else if (k - anchorStart >= maxTokenLength) end = -2
            else k += 1
          }
          if (end == -2) {
            state = ac.root // token too long → leave verbatim, keep scanning
          } else if (end >= 0) {
            replace(anchorStart, end)
            i = end - 1
            state = ac.root
          } else if (atEOF) {
            replace(anchorStart, len) // token runs to EOF → capture it whole
            i = len - 1
            state = ac.root
          } else {
            emit(lastEmit, anchorStart) // boundary not here yet → hold for next chunk
            return (out.result(), anchorStart)
          }
        }
      }
      i += 1
    }

    if (atEOF) {
      emit(lastEmit, len)
      (out.result(), len)
    } else {
      // hold back a possible partial anchor at the tail
      val safe = math.max(lastEmit, len - ac.depthAt(state))
      emit(lastEmit, safe)
      (out.result(), safe)
    }
  }
}

object TokenRewriter {

  /** A URL ends at whitespace/control, quotes, `<`, `>`, `]` (CDATA close) or a backtick. */
  val UrlBoundary: Byte => Boolean = b => {
    val u = b & 0xff
    u <= 0x20 || u == '"' || u == '\'' || u == '<' || u == '>' || u == ']' || u == '`'
  }

  /**
   * Wrap every URL starting with `anchor` using `template`, where `{url}` is the
   * captured URL verbatim and `{enc}` is its URL-encoded form. E.g.
   * {{{
   * wrappingUrls("https://tracker.example.com",
   *              "https://fp.publisher.com/collect?dest={enc}")
   * }}}
   */
  def wrappingUrls(anchor: String, template: String): TokenRewriter =
    new TokenRewriter(
      Seq(anchor),
      token =>
        template
          .replace("{enc}", java.net.URLEncoder.encode(token, "UTF-8"))
          .replace("{url}", token)
    )
}
