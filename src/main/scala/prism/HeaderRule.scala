package prism

import org.apache.pekko.http.scaladsl.model.HttpResponse
import org.apache.pekko.http.scaladsl.model.headers.{RawHeader, SameSite, `Set-Cookie`}

/**
 * A structured response-HEADER rewrite, applied to the parsed [[HttpResponse]] — not
 * to the body byte stream. Headers are not part of the text the body rewriters see
 * (Pekko parses them into a typed model before the entity), so cookie/header changes
 * live here, working on the model (`HttpCookie.withHttpOnly(...)`) rather than text.
 *
 *   - [[CookieFlags]]  enforce (or, for testing, strip) HttpOnly / Secure / SameSite
 *                      on every `Set-Cookie`. Enforcing is a defensive hardening you
 *                      can deploy; stripping weakens security and belongs only in an
 *                      authorized test harness, never on production user traffic.
 *   - [[SetHeader]]    add/replace a response header (e.g. CSP, HSTS, X-Frame-Options).
 *   - [[StripHeader]]  remove a response header (e.g. Server, X-Powered-By).
 */
sealed trait HeaderRule

object HeaderRule {

  final case class CookieFlags(httpOnly: Option[Boolean], secure: Option[Boolean], sameSite: Option[String])
      extends HeaderRule
  final case class SetHeader(name: String, value: String) extends HeaderRule
  final case class StripHeader(name: String)              extends HeaderRule

  /** Apply one header rule to a response. */
  def mutate(resp: HttpResponse, rule: HeaderRule): HttpResponse = rule match {
    case CookieFlags(httpOnly, secure, sameSite) =>
      resp.mapHeaders(_.map {
        case `Set-Cookie`(cookie) =>
          var c = cookie
          httpOnly.foreach(b => c = c.withHttpOnly(b))
          secure.foreach(b => c = c.withSecure(b))
          sameSite.flatMap(sameSiteOf).foreach(s => c = c.withSameSite(s))
          `Set-Cookie`(c)
        case other => other
      })

    case SetHeader(name, value) =>
      val lc = name.toLowerCase
      resp.withHeaders(resp.headers.filterNot(_.lowercaseName == lc) :+ RawHeader(name, value))

    case StripHeader(name) =>
      val lc = name.toLowerCase
      resp.withHeaders(resp.headers.filterNot(_.lowercaseName == lc))
  }

  /** Apply all rules in order. */
  def applyAll(resp: HttpResponse, rules: List[HeaderRule]): HttpResponse =
    rules.foldLeft(resp)(mutate)

  private def sameSiteOf(s: String): Option[SameSite] = s.toLowerCase match {
    case "lax"    => Some(SameSite.Lax)
    case "strict" => Some(SameSite.Strict)
    case "none"   => Some(SameSite.None)
    case _        => None
  }
}
