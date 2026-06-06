package prism

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString
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
 * A real, point-it-anywhere reverse proxy (usage #1 from the README).
 *
 * It forwards every incoming request to an origin, streams the response back,
 * and applies one or more byte-level rewrites to the HTML on the way through —
 * the body is never buffered, and a pattern split across TCP packets is still
 * rewritten thanks to [[RewriteFlow]]'s carry.
 *
 * {{{
 *   sbt "runMain prism.ReverseProxy <bindPort> <originBaseUrl> [options]"
 *
 *   options:
 *     --rewrite from=to       replace `from` with `to` everywhere in HTML bodies
 *                             (repeatable; e.g. swap an asset host onto your CDN)
 *     --rewrite-word from=to  like --rewrite but only on whole words, so `head`
 *                             does not match `header`/`ahead` (repeatable)
 *     --insert-before A=html  insert `html` immediately before each anchor `A`
 *                             (e.g. inject a script before `</head>`; repeatable)
 *     --insert-after  A=html  insert `html` immediately after each anchor `A`
 *                             (e.g. a banner right after `<body>`; repeatable)
 *     --wrap-url anchor=tmpl  capture each URL starting with `anchor` and replace
 *                             it with `tmpl`, where {url}=original, {enc}=url-encoded
 *                             (first-party tracker proxying; repeatable)
 *     --public-host host      sugar for --rewrite <origin-host>=host, i.e. rewrite
 *                             the origin's own host to the public one
 *     --attr                  apply the --rewrite swaps only inside URL attributes
 *                             (href/src/…), case-insensitively
 *     --text                  apply everything only to HTML text content — skip
 *                             tags, attributes, <script>/<style> and comments
 *                             (so even common words are safe to rewrite)
 *     --xml                   also rewrite XML responses (application/xml, text/xml,
 *                             …+xml) — e.g. VAST ad documents, not just HTML
 *     --tls ks.p12 password   serve HTTPS using a PKCS12 keystore (else plain HTTP)
 *     --pool-max-connections N      max TCP connections to the origin (default 64)
 *     --pool-max-open-requests N    max in-flight requests, power of 2 (default 256)
 *     --pool-min-connections N      warm connections kept open (default 0)
 *     --pool-idle-timeout 30s       close idle pooled connections after this
 *     --pool-pipelining-limit N     HTTP pipelining depth per connection
 *   (pool defaults live in application.conf; flags override per run.)
 * }}}
 *
 * For a local HTTPS test, make a self-signed keystore first:
 * {{{
 *   keytool -genkeypair -keystore proxy.p12 -storetype PKCS12 -storepass changeit \
 *     -keyalg RSA -keysize 2048 -validity 365 -alias proxy \
 *     -dname "CN=localhost" -ext SAN=dns:localhost
 *   ./run-proxy.sh 8443 http://localhost:9001 --tls proxy.p12 changeit …
 *   curl -k https://localhost:8443/
 * }}}
 *
 * Example — brand an R2 asset host behind your own CDN domain:
 * {{{
 *   sbt "runMain prism.ReverseProxy 8080 http://publisher.programmer.llc \
 *        --rewrite pub-abc123.r2.dev=cdn.publisher.programmer.llc"
 *   curl -s http://localhost:8080/   # R2 host now reads cdn.publisher.programmer.llc
 * }}}
 *
 * Non-HTML responses (images, CSS, JSON) pass through untouched — see [[RewriteHttp]].
 */
object ReverseProxy {

  /** Hop-by-hop headers that must not be blindly forwarded in either direction. */
  private val hopByHop: Set[String] =
    Set("connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailer", "transfer-encoding", "upgrade")

  def main(args: Array[String]): Unit = {
    // --- parse args: two positionals then options ---
    val positional   = ListBuffer[String]()
    val rewrites     = ListBuffer[(String, String)]()
    val wordRewrites = ListBuffer[(String, String)]()
    val insertRules  = ListBuffer[Rule]()             // insert-before / insert-after
    val wrapUrls     = ListBuffer[(String, String)]() // url-anchor to template ({url}/{enc})
    var publicHost   = Option.empty[String]
    var attrScoped   = false
    var textOnly     = false
    var acceptXml    = false // also rewrite application/xml, text/xml, …+xml (VAST)
    var tls          = Option.empty[(String, String)] // (keystore.p12, password)
    var maxConns     = Option.empty[Int]      // pool max connections to the origin
    var maxOpenReqs  = Option.empty[Int]      // pool max in-flight requests
    var minConns     = Option.empty[Int]      // pool warm connections
    var idleTimeout  = Option.empty[Duration] // close idle pooled connections after
    var pipelining   = Option.empty[Int]      // HTTP pipelining depth per connection

    val it = args.iterator
    def pair(opt: String): (String, String) =
      it.next().split("=", 2) match {
        case Array(from, to) if from.nonEmpty => from -> to
        case bad => sys.error(s"bad $opt '${bad.mkString("=")}', expected anchor=html")
      }
    while (it.hasNext) it.next() match {
      case "--rewrite"      if it.hasNext => rewrites += pair("--rewrite")
      case "--rewrite-word" if it.hasNext => wordRewrites += pair("--rewrite-word")
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

    // "host" or "host:port" exactly as it would appear in the origin's links
    val originAuthority = {
      val a = originUri.authority
      if (a.port > 0) s"${a.host.address}:${a.port}" else a.host.address
    }
    publicHost.foreach(h => rewrites += (originAuthority -> h))

    val literalPairs = rewrites.toList
    val wordPairs    = wordRewrites.toList
    val wrapPairs    = wrapUrls.toList

    // Collect the flags into the shared Rule model and let RuleFlow build the flow
    // (same code path as the config-driven ProxyServer).
    val rules: List[Rule] =
      literalPairs.map { case (f, t) => Rule.Rewrite(f, t) } :::
      wordPairs.map    { case (f, t) => Rule.RewriteWord(f, t) } :::
      wrapPairs.map    { case (a, t) => Rule.WrapUrl(a, t) } :::
      insertRules.toList
    require(
      rules.nonEmpty,
      "nothing to do: pass --rewrite, --rewrite-word, --wrap-url, --insert-*, or --public-host"
    )

    val rewriteFlow = RuleFlow.build(rules, textOnly, attrScoped)

    // Content-type gate: HTML only by default; --xml also admits XML (VAST/RSS/SOAP).
    val accept = if (acceptXml) RewriteHttp.isHtmlOrXml else RewriteHttp.isHtml

    implicit val system: ActorSystem[Nothing] =
      ActorSystem(Behaviors.empty, "reverse-proxy")
    import system.executionContext

    // Client connection-pool tuning for the origin. Defaults (max-connections=4,
    // max-open-requests=32) are the usual proxy bottleneck under concurrency.
    val poolSettings = {
      var s = ConnectionPoolSettings(system) // baseline from application.conf
      maxConns.foreach(n => s = s.withMaxConnections(n))
      maxOpenReqs.foreach(n => s = s.withMaxOpenRequests(n))
      minConns.foreach(n => s = s.withMinConnections(n))
      idleTimeout.foreach(d => s = s.withIdleTimeout(d))
      pipelining.foreach(n => s = s.withPipeliningLimit(n))
      s
    }

    def forwardableRequestHeaders(headers: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
      headers.filter { h => // drop synthetic (Timeout-Access, Remote-Address…), hop-by-hop, host
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
      val outgoing = req
        .withUri(target)
        .withHeaders(forwardableRequestHeaders(req.headers))

      Http()
        .singleRequest(outgoing, settings = poolSettings)
        // Serve on the client's protocol, not the origin's, BEFORE rewriting: a
        // rewritten (chunked) body must not inherit a stale HTTP/1.0 from the origin
        // (HTTP/1.0 + chunked is illegal and the check fires when the entity is set).
        .map(_.withProtocol(req.protocol))
        .map(RewriteHttp.rewriteResponseWith(rewriteFlow, accept)) // gate, gunzip, rewrite, re-frame
        .map(resp => resp.withHeaders(forwardableResponseHeaders(resp.headers)))
    }

    // Optional HTTPS: load a PKCS12 keystore into an SSLContext.
    val httpsContext: Option[HttpsConnectionContext] = tls.map { case (keystore, password) =>
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

    val serverAt = Http().newServerAt("0.0.0.0", bindPort)
    val server   = httpsContext.fold(serverAt)(serverAt.enableHttps)
    server.bind(handler).onComplete {
      case Success(_) =>
        println(s"reverse proxy:  $scheme://localhost:$bindPort/  ->  $originBase${if (textOnly) "  (text-only)" else ""}")
        literalPairs.foreach { case (f, t) => println(s"  rewrite       $f  ->  $t") }
        wordPairs.foreach    { case (f, t) => println(s"  rewrite-word  $f  ->  $t") }
        wrapPairs.foreach   { case (a, t) => println(s"  wrap-url      $a  ->  $t") }
        if (insertRules.nonEmpty) println(s"  insert        ${insertRules.size} anchor(s)")
        println(s"  pool          max-connections=${poolSettings.maxConnections}, max-open-requests=${poolSettings.maxOpenRequests}, " +
                s"min-connections=${poolSettings.minConnections}, pipelining=${poolSettings.pipeliningLimit}")
        println("Press Ctrl-C to stop.")
      case Failure(e) =>
        System.err.println(s"bind failed: ${e.getMessage}")
        system.terminate()
    }

    // Run headless: block until the system is terminated (Ctrl-C / SIGTERM kills it).
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
