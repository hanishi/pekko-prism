package prism

import com.typesafe.config.Config
import org.apache.pekko.http.scaladsl.model.{ContentType, HttpResponse, MediaTypes, Uri}
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
 * This is the whole behaviour of [[ProxyServer]] expressed as data — no CLI flags.
 */
final case class ProxyConfig(
    interface: String,
    port: Int,
    origin: Uri,
    healthPath: String,
    accept: ContentType => Boolean,
    textOnly: Boolean,
    tls: Option[(String, String)],
    rules: List[Rule],
    headerRules: List[HeaderRule]
) {

  /** Build the response-body rewrite flow these rules describe (identity if none). */
  def rewriteFlow: Flow[ByteString, ByteString, ?] = RuleFlow.build(rules, textOnly)

  /** Apply the structured response-header rules (cookie flags, set/strip header). */
  def applyHeaderRules(resp: HttpResponse): HttpResponse = HeaderRule.applyAll(resp, headerRules)
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

    // The `rules` list mixes body rules and header rules; split them by type.
    val (bodyRules, headerRules) =
      c.getConfigList("rules").asScala.toList.map(parseEntry).partitionMap(identity)

    ProxyConfig(
      interface   = c.getString("interface"),
      port        = c.getInt("port"),
      origin      = Uri(origin),
      healthPath  = c.getString("health-path"),
      accept      = acceptPredicate(c.getStringList("accept").asScala.toList),
      textOnly    = c.getBoolean("text-only"),
      tls         = tls,
      rules       = bodyRules,
      headerRules = headerRules
    )
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
