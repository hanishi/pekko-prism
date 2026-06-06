package prism

/**
 * Minimal, allocation-light HTML character-reference decoder, scoped to what an
 * attribute *value* realistically contains (mostly `&amp;` in URL query strings).
 *
 * It resolves the five predefined XML/HTML entities plus numeric references
 * (`&#38;`, `&#x26;`); anything it does not recognise is left byte-for-byte
 * intact, so an unescaped `&` or an exotic named entity is never mangled. This
 * is deliberately *not* the full WHATWG named-character-reference table — front
 * the rewriter with a real HTML parser if you need that.
 */
object HtmlEntities {

  private val named: Map[String, String] =
    Map("amp" -> "&", "lt" -> "<", "gt" -> ">", "quot" -> "\"", "apos" -> "'")

  /** Longest entity body we will look ahead for before giving up (`#x10FFFF`). */
  private val MaxBodyLen = 9

  def decode(s: String): String = {
    if (s.indexOf('&') < 0) return s // fast path: nothing to do

    val sb = new StringBuilder(s.length)
    val n  = s.length
    var i  = 0
    while (i < n) {
      val c = s.charAt(i)
      if (c == '&') {
        val semi = s.indexOf(';', i + 1)
        val resolved =
          if (semi > i && semi - (i + 1) <= MaxBodyLen) decodeOne(s.substring(i + 1, semi))
          else null
        if (resolved != null) { sb.append(resolved); i = semi + 1 }
        else { sb.append(c); i += 1 } // unknown / unterminated → keep the '&' literal
      } else { sb.append(c); i += 1 }
    }
    sb.toString
  }

  /** Decode one entity body (the text between `&` and `;`), or null if unknown. */
  private def decodeOne(body: String): String = {
    if (body.isEmpty) return null
    if (body.charAt(0) == '#') {
      try {
        val cp =
          if (body.length > 1 && (body.charAt(1) == 'x' || body.charAt(1) == 'X'))
            Integer.parseInt(body.substring(2), 16)
          else
            Integer.parseInt(body.substring(1), 10)
        if (Character.isValidCodePoint(cp)) new String(Character.toChars(cp)) else null
      } catch { case _: NumberFormatException => null }
    } else named.getOrElse(body, null)
  }
}
