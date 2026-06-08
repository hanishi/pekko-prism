package prism

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString
import prism.http.RewriteHttp

import java.io.{File, FileInputStream}
import java.security.{KeyStore, SecureRandom}
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicReference
import javax.net.ssl.{KeyManagerFactory, SSLContext}
import scala.collection.immutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
 * Config-driven reverse proxy: a real Pekko HTTP service whose behavior comes
 * entirely from a HOCON file (`prism.proxy` section), not CLI flags. See
 * `application.conf` for the schema and `docs/proxy-config.md` for the reference. Run:
 * {{{
 *   ./run-proxy-server.sh proxy.conf
 *   java -Dconfig.file=proxy.conf -cp <cp> prism.ProxyServer
 * }}}
 *
 * Production niceties: health endpoint, X-Forwarded-* headers, 502/504 on upstream
 * failure (no leaked stack traces), per-request access logging, and graceful drain on
 * SIGTERM.
 */
object ProxyServer {

  private val hopByHop: Set[String] =
    Set("connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailer", "transfer-encoding", "upgrade")

  /**
   * The request handler a config implies: proxy to the origin, rewrite the response,
   * add forwarding headers, answer the health path, and map upstream failures to
   * 502/504. Exposed (rather than inlined in [[main]]) so it can be tested directly.
   */
  def buildHandler(cfg: ProxyConfig, metrics: Metrics = new Metrics())(using system: ActorSystem[?]): HttpRequest => Future[HttpResponse] =
    handlerFor(() => cfg, metrics)

  /**
   * Core handler over a config *supplier*, read fresh on every request so a hot reload
   * (see [[main]]) takes effect without rebinding. The derived rewrite flow is cached
   * and rebuilt only when the config instance changes.
   */
  private[prism] def handlerFor(current: () => ProxyConfig, metrics: Metrics)(using system: ActorSystem[?]): HttpRequest => Future[HttpResponse] = {
    import system.executionContext
    val log          = org.slf4j.LoggerFactory.getLogger("prism.proxy")
    val poolSettings = ConnectionPoolSettings(system)
    val scheme       = if (current().tls.isDefined) "https" else "http" // TLS is fixed at startup

    val flowCache = new AtomicReference[(ProxyConfig, Flow[ByteString, ByteString, ?])]()
    def flowFor(cfg: ProxyConfig): Flow[ByteString, ByteString, ?] = {
      val c = flowCache.get()
      if (c != null && (c._1 eq cfg)) c._2
      else { val f = cfg.rewriteFlow; flowCache.set((cfg, f)); f }
    }

    def keep(h: HttpHeader, rendersHere: Boolean, more: String => Boolean): Boolean =
      rendersHere && !hopByHop(h.lowercaseName) && more(h.lowercaseName)

    def fwdRequestHeaders(hs: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
      hs.filter(h => keep(h, h.renderInRequests(), n => n != "host" && n != "accept-encoding"))

    def fwdResponseHeaders(hs: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
      hs.filter(h => keep(h, h.renderInResponses(), _ => true))

    def forwardedHeaders(req: HttpRequest, cfg: ProxyConfig): List[HttpHeader] = {
      val clientIp = req.attribute(AttributeKeys.remoteAddress).flatMap(_.toOption).map(_.getHostAddress)
      val priorXff = req.headers.collectFirst { case h if h.lowercaseName == "x-forwarded-for" => h.value }
      val xff      = (priorXff.toList ++ clientIp.toList).mkString(", ")
      val host     = req.headers.collectFirst { case h if h.lowercaseName == "host" => h.value }
                        .getOrElse(cfg.origin.authority.host.address)
      List(
        Option.when(xff.nonEmpty)(RawHeader("X-Forwarded-For", xff)),
        Some(RawHeader("X-Forwarded-Proto", scheme)),
        Some(RawHeader("X-Forwarded-Host", host)),
        Some(RawHeader("Via", "1.1 prism"))
      ).flatten
    }

    def errorResponse(status: StatusCode, msg: String): HttpResponse =
      HttpResponse(status, entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, msg + "\n"))

    def proxy(req: HttpRequest, cfg: ProxyConfig): Future[HttpResponse] = {
      val withPath = cfg.origin.withPath(req.uri.path)
      val target   = req.uri.rawQueryString.fold(withPath)(withPath.withRawQueryString)
      val outgoing = req.withUri(target).withHeaders(fwdRequestHeaders(req.headers) ++ forwardedHeaders(req, cfg))

      Http()
        .singleRequest(outgoing, settings = poolSettings)
        .map(_.withProtocol(req.protocol)) // serve client's protocol, not origin's (1.0+chunked is illegal)
        .map(RewriteHttp.rewriteResponseWith(flowFor(cfg), cfg.accept))
        .map(resp => resp.withHeaders(fwdResponseHeaders(resp.headers)))
        .map(cfg.applyHeaderRules) // structured header/cookie rules, last (survive header filtering)
        .recover {
          case _: TimeoutException =>
            log.warn("upstream timeout for {}", req.uri.path)
            errorResponse(StatusCodes.GatewayTimeout, "upstream timeout")
          case e =>
            log.warn("upstream error for {}: {}", req.uri.path, e.getMessage)
            errorResponse(StatusCodes.BadGateway, "bad gateway")
        }
    }

    req => {
      val cfg   = current()
      val start = System.nanoTime()
      val path  = req.uri.path.toString
      val result =
        if (path == cfg.healthPath)
          Future.successful(HttpResponse(StatusCodes.OK, entity = "ok\n"))
        else if (cfg.metricsPath.contains(path))
          Future.successful(HttpResponse(StatusCodes.OK,
            entity = HttpEntity(ContentTypes.`text/plain(UTF-8)`, metrics.render())))
        else proxy(req, cfg)
      result.map { resp =>
        val nanos = System.nanoTime() - start
        log.info("{} {} -> {} {}ms", req.method.value, req.uri.path, resp.status.intValue, nanos / 1000000)
        if (!cfg.metricsPath.contains(path)) metrics.record(resp.status.intValue, nanos / 1e9)
        resp
      }
    }
  }

  /** Build the optional HTTPS context from a PKCS12 keystore. */
  def httpsContext(cfg: ProxyConfig): Option[HttpsConnectionContext] = cfg.tls.map { case (keystore, password) =>
    val ks = KeyStore.getInstance("PKCS12")
    val in = new FileInputStream(keystore)
    try ks.load(in, password.toCharArray) finally in.close()
    val kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    kmf.init(ks, password.toCharArray)
    val ctx = SSLContext.getInstance("TLS")
    ctx.init(kmf.getKeyManagers, null, new SecureRandom())
    ConnectionContext.httpsServer(ctx)
  }

  /** Poll the config file's mtime and hot-swap the live config when it changes. */
  private def startConfigWatcher(path: String, cfgRef: AtomicReference[ProxyConfig], log: org.slf4j.Logger): Unit = {
    val file = new File(path)
    val t = new Thread(() => {
      var last = file.lastModified()
      while (!Thread.currentThread().isInterrupted) {
        try Thread.sleep(3000) catch { case _: InterruptedException => return }
        val now = file.lastModified()
        if (now != 0L && now != last) {
          last = now
          try {
            val parsed = ConfigFactory.parseFile(file).withFallback(ConfigFactory.load()).resolve().getConfig("prism.proxy")
            val next   = ProxyConfig.from(parsed)
            cfgRef.set(next)
            log.info("config reloaded from {} ({} body + {} header rule(s))", path, next.rules.size, next.headerRules.size)
          } catch {
            case e: Throwable => log.warn("config reload failed, keeping previous config: {}", e.getMessage)
          }
        }
      }
    }, "prism-config-watcher")
    t.setDaemon(true)
    t.start()
  }

  def main(args: Array[String]): Unit = {
    // Load config: explicit file arg, else standard ConfigFactory (honours -Dconfig.file).
    val config = args.headOption match {
      case Some(path) =>
        ConfigFactory.parseFile(new File(path)).withFallback(ConfigFactory.load()).resolve()
      case None => ConfigFactory.load()
    }
    val cfg    = ProxyConfig.from(config.getConfig("prism.proxy"))
    val cfgRef = new AtomicReference(cfg)

    implicit val system: ActorSystem[Nothing] =
      ActorSystem(Behaviors.empty, "prism-proxy", config)
    import system.executionContext
    val log          = org.slf4j.LoggerFactory.getLogger("prism.proxy")
    val poolSettings = ConnectionPoolSettings(system)
    val scheme       = if (cfg.tls.isDefined) "https" else "http"

    // Hot reload: if enabled and started from a file, watch it and swap the live config.
    args.headOption.filter(_ => cfg.reload).foreach(startConfigWatcher(_, cfgRef, log))

    val serverAt = Http().newServerAt(cfg.interface, cfg.port)
    val server   = httpsContext(cfg).fold(serverAt)(serverAt.enableHttps)

    server.bind(handlerFor(() => cfgRef.get, new Metrics())).onComplete {
      case Success(binding) =>
        log.info("prism proxy {}://{}:{}/  ->  {}  ({} body + {} header rule(s), pool {}/{}{})",
          scheme, cfg.interface, cfg.port, cfg.origin,
          cfg.rules.size, cfg.headerRules.size, poolSettings.maxConnections, poolSettings.maxOpenRequests,
          if (cfg.reload) ", hot-reload on" else "")
        // Graceful drain on SIGTERM / Ctrl-C: stop accepting, finish in-flight, then exit.
        sys.addShutdownHook {
          log.info("draining…")
          Await.result(binding.terminate(hardDeadline = 10.seconds), 15.seconds)
          system.terminate()
        }
      case Failure(e) =>
        log.error("bind failed: {}", e.getMessage)
        system.terminate()
    }

    Await.result(system.whenTerminated, Duration.Inf)
  }
}
