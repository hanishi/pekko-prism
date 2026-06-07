package prism

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import prism.http.RewriteHttp

import java.io.FileInputStream
import java.security.{KeyStore, SecureRandom}
import javax.net.ssl.{KeyManagerFactory, SSLContext}
import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
 * A flag-driven CLI reverse proxy: the quick-experiment counterpart to the
 * config-driven [[ProxyServer]]. It forwards every request to an origin, streams the
 * response back, and applies byte-level rewrites on the way through. The body is never
 * buffered, and a pattern split across TCP packets is still rewritten thanks to the
 * carry in [[RewriteFlow]].
 *
 * {{{
 *   sbt "runMain prism.ReverseProxy <bindPort> <originBaseUrl> [options]"
 *
 *   options:
 *     --rewrite from=to       replace `from` with `to` everywhere in HTML bodies
 *     --rewrite-word from=to  like --rewrite but only on whole words
 *     --insert-before A=html  insert `html` immediately before each anchor `A`
 *     --insert-after  A=html  insert `html` immediately after each anchor `A`
 *     --wrap-url anchor=tmpl  capture each URL starting with `anchor`, replace with
 *                             `tmpl` ({url}=original, {enc}=url-encoded)
 *     --public-host host      sugar for --rewrite <origin-host>=host
 *     --attr                  apply the --rewrite swaps only inside URL attributes
 *     --text                  apply everything only to HTML text content (skip tags,
 *                             attributes, <script>/<style> and comments)
 *     --xml                   also rewrite XML responses (VAST/RSS/SOAP), not just HTML
 *     --tls ks.p12 password   serve HTTPS using a PKCS12 keystore (else plain HTTP)
 *     --pool-max-connections N      max TCP connections to the origin (default 64)
 *     --pool-max-open-requests N    max in-flight requests, power of 2 (default 256)
 *     --pool-min-connections N      warm connections kept open (default 0)
 *     --pool-idle-timeout 30s       close idle pooled connections after this
 *     --pool-pipelining-limit N     HTTP pipelining depth per connection
 * }}}
 *
 * Non-HTML responses (images, CSS, JSON) pass through untouched (see [[RewriteHttp]]).
 * For the full feature set and a config file instead of flags, use [[ProxyServer]].
 */
object ReverseProxy {

  /** Hop-by-hop headers that must not be blindly forwarded in either direction. */
  private val hopByHop: Set[String] =
    Set("connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailer", "transfer-encoding", "upgrade")

  /** Parsed CLI arguments. Exposed so the flag parsing can be tested without binding. */
  final case class Cli(
      bindPort: Int,
      originBase: String,
      rules: List[Rule],
      textOnly: Boolean,
      attrScoped: Boolean,
      acceptXml: Boolean,
      tls: Option[(String, String)],
      maxConns: Option[Int],
      maxOpenReqs: Option[Int],
      minConns: Option[Int],
      idleTimeout: Option[Duration],
      pipelining: Option[Int]
  )

  /** Parse the CLI args: two positionals (port, origin) then options. */
  def parse(args: Array[String]): Cli = {
    val positional   = ListBuffer[String]()
    val rewrites     = ListBuffer[(String, String)]()
    val wordRewrites = ListBuffer[(String, String)]()
    val insertRules  = ListBuffer[Rule]()
    val wrapUrls     = ListBuffer[(String, String)]()
    var publicHost   = Option.empty[String]
    var attrScoped   = false
    var textOnly     = false
    var acceptXml    = false
    var tls          = Option.empty[(String, String)]
    var maxConns     = Option.empty[Int]
    var maxOpenReqs  = Option.empty[Int]
    var minConns     = Option.empty[Int]
    var idleTimeout  = Option.empty[Duration]
    var pipelining   = Option.empty[Int]

    val it = args.iterator
    def pair(opt: String): (String, String) =
      it.next().split("=", 2) match {
        case Array(from, to) if from.nonEmpty => from -> to
        case bad => sys.error(s"bad $opt '${bad.mkString("=")}', expected anchor=html")
      }
    while (it.hasNext) it.next() match {
      case "--rewrite"       if it.hasNext => rewrites += pair("--rewrite")
      case "--rewrite-word"  if it.hasNext => wordRewrites += pair("--rewrite-word")
      case "--insert-before" if it.hasNext => val (a, h) = pair("--insert-before"); insertRules += Rule.InsertBefore(a, h)
      case "--insert-after"  if it.hasNext => val (a, h) = pair("--insert-after");  insertRules += Rule.InsertAfter(a, h)
      case "--wrap-url"      if it.hasNext => wrapUrls += pair("--wrap-url")
      case "--public-host"  if it.hasNext => publicHost = Some(it.next())
      case "--attr"                       => attrScoped = true
      case "--text"                       => textOnly = true
      case "--xml"                        => acceptXml = true
      case "--tls" if it.hasNext =>
        val ks = it.next()
        val pw = if (it.hasNext) it.next() else sys.error("--tls needs: <keystore.p12> <password>")
        tls = Some((ks, pw))
      case "--pool-max-connections"   if it.hasNext => maxConns = Some(it.next().toInt)
      case "--pool-max-open-requests" if it.hasNext => maxOpenReqs = Some(it.next().toInt)
      case "--pool-min-connections"   if it.hasNext => minConns = Some(it.next().toInt)
      case "--pool-idle-timeout"      if it.hasNext => idleTimeout = Some(Duration(it.next()))
      case "--pool-pipelining-limit"  if it.hasNext => pipelining = Some(it.next().toInt)
      case opt if opt.startsWith("--")    => sys.error(s"unknown option: $opt")
      case other                          => positional += other
    }

    val bindPort   = positional.headOption.map(_.toInt).getOrElse(8080)
    val originBase = positional.lift(1).getOrElse("http://localhost:9001")
    val originUri  = Uri(originBase)
    val originAuthority = {
      val a = originUri.authority
      if (a.port > 0) s"${a.host.address}:${a.port}" else a.host.address
    }
    publicHost.foreach(h => rewrites += (originAuthority -> h))

    val rules: List[Rule] =
      rewrites.toList.map { case (f, t) => Rule.Rewrite(f, t) } :::
      wordRewrites.toList.map { case (f, t) => Rule.RewriteWord(f, t) } :::
      wrapUrls.toList.map { case (a, t) => Rule.WrapUrl(a, t) } :::
      insertRules.toList
    require(
      rules.nonEmpty,
      "nothing to do: pass --rewrite, --rewrite-word, --wrap-url, --insert-*, or --public-host"
    )

    Cli(bindPort, originBase, rules, textOnly, attrScoped, acceptXml, tls,
        maxConns, maxOpenReqs, minConns, idleTimeout, pipelining)
  }

  def main(args: Array[String]): Unit = {
    val cli         = parse(args)
    val originUri   = Uri(cli.originBase)
    val rewriteFlow = RuleFlow.build(cli.rules, cli.textOnly, cli.attrScoped)
    val accept      = if (cli.acceptXml) RewriteHttp.isHtmlOrXml else RewriteHttp.isHtml

    implicit val system: ActorSystem[Nothing] =
      ActorSystem(Behaviors.empty, "reverse-proxy")
    import system.executionContext

    val poolSettings = {
      var s = ConnectionPoolSettings(system)
      cli.maxConns.foreach(n => s = s.withMaxConnections(n))
      cli.maxOpenReqs.foreach(n => s = s.withMaxOpenRequests(n))
      cli.minConns.foreach(n => s = s.withMinConnections(n))
      cli.idleTimeout.foreach(d => s = s.withIdleTimeout(d))
      cli.pipelining.foreach(n => s = s.withPipeliningLimit(n))
      s
    }

    def forwardableRequestHeaders(headers: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
      headers.filter { h =>
        val n = h.lowercaseName
        h.renderInRequests() && !hopByHop(n) && n != "host" && n != "accept-encoding"
      }
    def forwardableResponseHeaders(headers: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
      headers.filter(h => h.renderInResponses() && !hopByHop(h.lowercaseName))

    val handler: HttpRequest => Future[HttpResponse] = { req =>
      val withPath = originUri.withPath(req.uri.path)
      val target = req.uri.rawQueryString match {
        case Some(q) => withPath.withRawQueryString(q)
        case None    => withPath
      }
      val outgoing = req.withUri(target).withHeaders(forwardableRequestHeaders(req.headers))
      Http()
        .singleRequest(outgoing, settings = poolSettings)
        .map(_.withProtocol(req.protocol)) // serve client's protocol (1.0 + chunked is illegal)
        .map(RewriteHttp.rewriteResponseWith(rewriteFlow, accept))
        .map(resp => resp.withHeaders(forwardableResponseHeaders(resp.headers)))
    }

    val httpsContext: Option[HttpsConnectionContext] = cli.tls.map { case (keystore, password) =>
      val ks = KeyStore.getInstance("PKCS12")
      val in = new FileInputStream(keystore)
      try ks.load(in, password.toCharArray) finally in.close()
      val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
      kmf.init(ks, password.toCharArray)
      val ctx = SSLContext.getInstance("TLS")
      ctx.init(kmf.getKeyManagers, null, new SecureRandom())
      ConnectionContext.httpsServer(ctx)
    }
    val scheme = if (httpsContext.isDefined) "https" else "http"

    val serverAt = Http().newServerAt("0.0.0.0", cli.bindPort)
    val server   = httpsContext.fold(serverAt)(serverAt.enableHttps)
    server.bind(handler).onComplete {
      case Success(_) =>
        println(s"reverse proxy:  $scheme://localhost:${cli.bindPort}/  ->  ${cli.originBase}${if (cli.textOnly) "  (text-only)" else ""}")
        cli.rules.foreach {
          case Rule.Rewrite(f, t)     => println(s"  rewrite       $f  ->  $t")
          case Rule.RewriteWord(f, t) => println(s"  rewrite-word  $f  ->  $t")
          case Rule.WrapUrl(a, t)     => println(s"  wrap-url      $a  ->  $t")
          case _                      => ()
        }
        val inserts = cli.rules.count { case _: Rule.InsertBefore | _: Rule.InsertAfter => true; case _ => false }
        if (inserts > 0) println(s"  insert        $inserts anchor(s)")
        println(s"  pool          max-connections=${poolSettings.maxConnections}, max-open-requests=${poolSettings.maxOpenRequests}, " +
                s"min-connections=${poolSettings.minConnections}, pipelining=${poolSettings.pipeliningLimit}")
        println("Press Ctrl-C to stop.")
      case Failure(e) =>
        System.err.println(s"bind failed: ${e.getMessage}")
        system.terminate()
    }

    Await.result(system.whenTerminated, Duration.Inf)
  }
}
