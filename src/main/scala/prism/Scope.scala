package prism

import org.apache.pekko.http.scaladsl.model.HttpRequest

/**
 * A predicate over a request. A rule carrying a non-empty scope applies only when the
 * request matches all of the conditions present; [[Scope.any]] (no conditions) always
 * matches. Conditions are AND-ed.
 *
 * @param path   request path; a trailing `*` is a prefix wildcard, otherwise exact
 * @param host   request Host (port ignored), case-insensitive
 * @param method HTTP method, case-insensitive
 */
final case class Scope(
    path: Option[String] = None,
    host: Option[String] = None,
    method: Option[String] = None
) {
  def matches(req: HttpRequest): Boolean = {
    val pathOk = path.forall(p => Scope.globMatch(p, req.uri.path.toString))
    val hostOk = host.forall { h =>
      val seen = req.headers.collectFirst { case x if x.lowercaseName == "host" => x.value }
        .orElse(Option(req.uri.authority.host.address).filter(_.nonEmpty))
      seen.map(_.takeWhile(_ != ':')).exists(_.equalsIgnoreCase(h))
    }
    val methodOk = method.forall(_.equalsIgnoreCase(req.method.value))
    pathOk && hostOk && methodOk
  }
}

object Scope {
  val any: Scope = Scope()

  /** Glob match: a trailing `*` is a prefix wildcard, otherwise an exact match. */
  def globMatch(pattern: String, path: String): Boolean =
    if (pattern.endsWith("*")) path.startsWith(pattern.dropRight(1))
    else pattern == path
}

/** A body [[Rule]] paired with the [[Scope]] in which it applies. */
final case class ScopedRule(rule: Rule, scope: Scope)

/** A [[HeaderRule]] paired with the [[Scope]] in which it applies. */
final case class ScopedHeaderRule(rule: HeaderRule, scope: Scope)
