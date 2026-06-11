package prism

import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.model.{ContentType, HttpRequest, HttpResponse, MediaTypes, Uri}
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString
import prism.http.RewriteHttp

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*

/** One declarative response-BODY rewrite rule from the config file. */
sealed trait Rule
object Rule {
  case class Rewrite(from: String, to: String)          extends Rule
  case class RewriteWord(from: String, to: String)      extends Rule
  case class WrapUrl(anchor: String, template: String)  extends Rule
  case class InsertBefore(anchor: String, html: String) extends Rule
  case class InsertAfter(anchor: String, html: String)  extends Rule
}

/**
 * Typed view of the `prism.proxy` config section, plus the byte-flow it implies.
 * This is the whole behavior of [[ProxyServer]] expressed as data, no CLI flags.
 */
final case class ProxyConfig(
    interface: String,
    port: Int,
    origin: Uri,
    healthPath: String,
    metricsPath: Option[String],
    reload: Boolean,
    handleOptions: Boolean,
    accept: ContentType => Boolean,
    textOnly: Boolean,
    tls: Option[(String, String)],
    scopedRules: List[ScopedRule],
    scopedHeaderRules: List[ScopedHeaderRule],
    requestBodyRules: List[Rule],
    requestHeaderRules: List[HeaderRule]
) {

  /** All body rules, scope aside (the unscoped fast path and tests use this). */
  def rules: List[Rule] = scopedRules.map(_.rule)

  /** All header rules, scope aside. */
  def headerRules: List[HeaderRule] = scopedHeaderRules.map(_.rule)

  /** True if any rule is restricted to a [[Scope]]; if not, the flow is request-independent. */
  val hasScopes: Boolean = (scopedRules.map(_.scope) ++ scopedHeaderRules.map(_.scope)).exists(_ != Scope.any)

  /** The response-body rewrite flow for all rules (request-independent; identity if none). */
  def rewriteFlow: Flow[ByteString, ByteString, ?] = RuleFlow.build(rules, textOnly)

  /** The response-body rewrite flow for the rules whose scope matches `req`. */
  def rewriteFlowFor(req: HttpRequest): Flow[ByteString, ByteString, ?] =
    if (!hasScopes) rewriteFlow
    else RuleFlow.build(scopedRules.filter(_.scope.matches(req)).map(_.rule), textOnly)

  /** Apply all response-header rules (request-independent). */
  def applyHeaderRules(resp: HttpResponse): HttpResponse = HeaderRule.applyAll(resp, headerRules)

  /** Apply the response-header rules whose scope matches `req`. */
  def applyHeaderRulesFor(req: HttpRequest, resp: HttpResponse): HttpResponse =
    if (!hasScopes) applyHeaderRules(resp)
    else HeaderRule.applyAll(resp, scopedHeaderRules.filter(_.scope.matches(req)).map(_.rule))

  /** True if any request-side rule is configured. */
  val hasRequestRules: Boolean = requestBodyRules.nonEmpty || requestHeaderRules.nonEmpty

  /** Body rewrite flow applied to the *request* before forwarding. */
  def requestRewriteFlow: Flow[ByteString, ByteString, ?] = RuleFlow.build(requestBodyRules, textOnly = false)

  /**
   * Apply request-side rules to the outgoing request: `set-header`/`strip-header` on the
   * request headers (`cookie-flags` is response-only and ignored here), and body
   * `rewrite`/`rewrite-word` on the request entity (gated by `accept`, like responses).
   */
  def applyRequestRules(req: HttpRequest): HttpRequest =
    if (!hasRequestRules) req
    else {
      val headers = requestHeaderRules.foldLeft(req.headers) {
        case (hs, HeaderRule.SetHeader(n, v)) => hs.filterNot(_.lowercaseName == n.toLowerCase) :+ RawHeader(n, v)
        case (hs, HeaderRule.StripHeader(n))  => hs.filterNot(_.lowercaseName == n.toLowerCase)
        case (hs, _: HeaderRule.CookieFlags)  => hs // not applicable to a request
      }
      val withHeaders = req.withHeaders(headers)
      if (requestBodyRules.isEmpty || !accept(req.entity.contentType)) withHeaders
      else withHeaders.withEntity(withHeaders.entity.transformDataBytes(requestRewriteFlow))
    }
}

/** Turns a list of [[Rule]]s into the byte-rewriting flow they describe. */
object RuleFlow {

  /**
   * True if no pattern is a (contiguous) substring of another. Under this condition
   * Wu-Manber's leftmost-longest-by-start selection is identical to Aho-Corasick's
   * leftmost-by-end, so the two produce the same output and we may dispatch to the
   * faster one. Divergence is only possible when one match's span contains another's,
   * which requires substring containment.
   *
   * `p == q` is allowed only because [[build]] dedupes equal `from`s first — with
   * true duplicates in the list, Wu-Manber would pick the last while Aho-Corasick
   * picks the first.
   */
  private[prism] def independent(ps: List[String]): Boolean =
    ps.forall(p => ps.forall(q => p == q || !q.contains(p)))

  def build(rules: List[Rule], textOnly: Boolean): Flow[ByteString, ByteString, ?] = {
    // Dedup by `from`, first rule wins. Aho-Corasick already keeps the first of two
    // equal patterns (a trie terminal is only displaced by a strictly longer one), but
    // Wu-Manber's candidate lists would let the LAST duplicate win — dedup before
    // dispatch keeps every matcher's selection identical. A duplicate that collapses
    // to a single rule also gets the faster BMH path.
    val literal = rules.collect { case Rule.Rewrite(f, t) => (f, t) }.distinctBy(_._1)
    val words   = rules.collect { case Rule.RewriteWord(f, t) => (f, t) }.distinctBy(_._1)
    val wraps   = rules.collect { case Rule.WrapUrl(a, t) => (a, t) }
    // Aho-Corasick keeps one pattern per `from`, so several inserts on one anchor must
    // be merged into a single replacement, not listed (a list would keep only the
    // first): befores in rule order, then the anchor, then afters in rule order.
    val insertAnchors = rules.collect {
      case Rule.InsertBefore(anchor, _) => anchor
      case Rule.InsertAfter(anchor, _)  => anchor
    }.distinct
    val inserts = insertAnchors.map { anchor =>
      val before = rules.collect { case Rule.InsertBefore(`anchor`, h) => h }.mkString
      val after  = rules.collect { case Rule.InsertAfter(`anchor`, h) => h }.mkString
      (anchor, before + anchor + after)
    }

    val content = ListBuffer[Rewriter]()
    // Pick the fastest correct matcher: one pattern -> Boyer-Moore-Horspool (~9x);
    // several independent patterns -> Wu-Manber (~4x); otherwise Aho-Corasick.
    if (literal.nonEmpty)
      content += {
        val froms = literal.map(_._1)
        if (literal.sizeIs == 1) BmhRewriter(literal.head._1, literal.head._2)
        else if (froms.forall(_.length >= 2) && RuleFlow.independent(froms)) new WuManberRewriter(literal)
        else new LiteralRewriter(literal)
      }
    if (words.nonEmpty)   content += new WordLiteralRewriter(words)

    // --text confines content rewrites to HTML text nodes; wrap-url and insert are
    // always whole-body (URLs live in attributes/CDATA; insert anchors are markup).
    val lift: Rewriter => Flow[ByteString, ByteString, ?] =
      if (textOnly) HtmlTextRewriteFlow(_) else RewriteFlow(_)

    val stages = ListBuffer[Flow[ByteString, ByteString, ?]]()
    content.foreach(r => stages += lift(r))
    wraps.foreach { case (a, t) => stages += RewriteFlow(TokenRewriter.wrappingUrls(a, t)) }
    if (inserts.nonEmpty) stages += RewriteFlow(new LiteralRewriter(inserts))

    if (stages.isEmpty) Flow[ByteString] else stages.reduce(_ via _)
  }
}

object ProxyConfig {

  /** Parse the `prism.proxy` section. */
  def from(c: Config): ProxyConfig = {
    val origin = c.getString("origin")
    require(origin.nonEmpty, "prism.proxy.origin must be set")

    val tlsCfg = c.getConfig("tls")
    val tls =
      if (tlsCfg.getBoolean("enabled"))
        Some((tlsCfg.getString("keystore"), tlsCfg.getString("password")))
      else None

    // The `rules` list mixes body rules and header rules (each optionally `when`-scoped);
    // split them by type, carrying each rule's scope.
    val (bodyRules, headerRules) =
      c.getConfigList("rules").asScala.toList.map { e =>
        val scope = parseScope(e)
        parseEntry(e) match {
          case Left(r)  => Left(ScopedRule(r, scope))
          case Right(h) => Right(ScopedHeaderRule(h, scope))
        }
      }.partitionMap(identity)

    // Request-side rules (applied to the outgoing request); unscoped, body + header.
    val (reqBody, reqHeader) =
      c.getConfigList("request-rules").asScala.toList.map(parseEntry).partitionMap(identity)

    ProxyConfig(
      interface          = c.getString("interface"),
      port               = c.getInt("port"),
      origin             = Uri(origin),
      healthPath         = c.getString("health-path"),
      metricsPath        = Some(c.getString("metrics-path")).filter(_.nonEmpty),
      reload             = c.getBoolean("reload"),
      handleOptions      = c.getBoolean("handle-options"),
      accept             = acceptPredicate(c.getStringList("accept").asScala.toList),
      textOnly           = c.getBoolean("text-only"),
      tls                = tls,
      scopedRules        = bodyRules,
      scopedHeaderRules  = headerRules,
      requestBodyRules   = reqBody,
      requestHeaderRules = reqHeader
    )
  }

  /** Parse an optional `when { path, host, method }` scope on a rule entry. */
  private def parseScope(c: Config): Scope =
    if (!c.hasPath("when")) Scope.any
    else {
      val w = c.getConfig("when")
      Scope(optStr(w, "path"), optStr(w, "host"), optStr(w, "method"))
    }

  private def optBool(c: Config, p: String): Option[Boolean] =
    if (c.hasPath(p)) Some(c.getBoolean(p)) else None
  private def optStr(c: Config, p: String): Option[String] =
    if (c.hasPath(p)) Some(c.getString(p)) else None

  /** Parse one config entry into a body rule (Left) or a header rule (Right). */
  private def parseEntry(c: Config): Either[Rule, HeaderRule] = c.getString("type") match {
    case "rewrite"       => Left(Rule.Rewrite(c.getString("from"), c.getString("to")))
    case "rewrite-word"  => Left(Rule.RewriteWord(c.getString("from"), c.getString("to")))
    case "wrap-url"      => Left(Rule.WrapUrl(c.getString("anchor"), c.getString("template")))
    case "insert-before" => Left(Rule.InsertBefore(c.getString("anchor"), c.getString("html")))
    case "insert-after"  => Left(Rule.InsertAfter(c.getString("anchor"), c.getString("html")))
    case "cookie-flags"  => Right(HeaderRule.CookieFlags(optBool(c, "http-only"), optBool(c, "secure"), optStr(c, "same-site")))
    case "set-header"    => Right(HeaderRule.SetHeader(c.getString("name"), c.getString("value")))
    case "strip-header"  => Right(HeaderRule.StripHeader(c.getString("name")))
    case other           => sys.error(s"unknown rule type: $other")
  }

  /** Turn `accept = ["html","xml","text/css","all"]` into a content-type gate. */
  private def acceptPredicate(types: List[String]): ContentType => Boolean = {
    val lc = types.map(_.toLowerCase)
    if (lc.contains("all") || lc.contains("*")) _ => true
    else {
      val html     = lc.contains("html")
      val xml      = lc.contains("xml")
      val explicit = lc.filter(_.contains("/")).toSet
      ct => {
        val mt = ct.mediaType
        (html && mt == MediaTypes.`text/html`) ||
        (xml && RewriteHttp.isXml(mt)) ||
        explicit.contains(s"${mt.mainType}/${mt.subType}")
      }
    }
  }
}
