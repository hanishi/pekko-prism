package prism

import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.model.{ContentType, HttpRequest, HttpResponse, MediaTypes, Uri}
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
    accept: ContentType => Boolean,
    textOnly: Boolean,
    tls: Option[(String, String)],
    scopedRules: List[ScopedRule],
    scopedHeaderRules: List[ScopedHeaderRule]
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
}

/** Turns a list of [[Rule]]s into the byte-rewriting flow they describe. */
object RuleFlow {
  def build(rules: List[Rule], textOnly: Boolean): Flow[ByteString, ByteString, ?] = {
    val literal = rules.collect { case Rule.Rewrite(f, t) => (f, t) }
    val words   = rules.collect { case Rule.RewriteWord(f, t) => (f, t) }
    val wraps   = rules.collect { case Rule.WrapUrl(a, t) => (a, t) }
    val inserts = rules.collect {
      case Rule.InsertBefore(a, h) => (a, h + a)
      case Rule.InsertAfter(a, h)  => (a, a + h)
    }

    val content = ListBuffer[Rewriter]()
    if (literal.nonEmpty) content += new LiteralRewriter(literal)
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

    ProxyConfig(
      interface         = c.getString("interface"),
      port              = c.getInt("port"),
      origin            = Uri(origin),
      healthPath        = c.getString("health-path"),
      metricsPath       = Some(c.getString("metrics-path")).filter(_.nonEmpty),
      reload            = c.getBoolean("reload"),
      accept            = acceptPredicate(c.getStringList("accept").asScala.toList),
      textOnly          = c.getBoolean("text-only"),
      tls               = tls,
      scopedRules       = bodyRules,
      scopedHeaderRules = headerRules
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
