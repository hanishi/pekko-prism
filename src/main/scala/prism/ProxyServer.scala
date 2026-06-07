package prism

import com.typesafe.config.ConfigFactory
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.RawHeader
import org.apache.pekko.http.scaladsl.settings.ConnectionPoolSettings
import org.apache.pekko.http.scaladsl.{ConnectionContext, Http, HttpsConnectionContext}
import prism.http.RewriteHttp

import java.io.{File, FileInputStream}
import java.security.{KeyStore, SecureRandom}
import java.util.concurrent.TimeoutException
import javax.net.ssl.{KeyManagerFactory, SSLContext}
import scala.collection.immutable
import scala.concurrent.duration.*
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

/**
 * Config-driven reverse proxy: a real Pekko HTTP service whose behaviour comes
 * entirely from a HOCON file (`prism.proxy` section), not CLI flags. See
 * `application.conf` for the schema and `docs/proxy-config.md` for the reference. Run:
 * {{{
 *   ./run-proxy-server.sh proxy.conf
 *   java -Dconfig.file=proxy.conf -cp <cp> prism.ProxyServer
 * }}}
 *
 * Production niceties over the [[ReverseProxy]] demo: health endpoint, X-Forwarded-*
 * headers, 502/504 on upstream failure (no leaked stack traces), per-request access
 * logging, and graceful drain on SIGTERM.
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
  def buildHandler(cfg: ProxyConfig)(using system: ActorSystem[?]): HttpRequest => Future[HttpResponse] = {
    import system.executionContext
    val log          = org.slf4j.LoggerFactory.getLogger("prism.proxy")
    val poolSettings = ConnectionPoolSettings(system)
    val rewriteFlow  = cfg.rewriteFlow
    val scheme       = if (cfg.tls.isDefined) "https" else "http"

    def keep(h: HttpHeader, rendersHere: Boolean, more: String => Boolean): Boolean =
      rendersHere && !hopByHop(h.lowercaseName) && more(h.lowercaseName)

    def fwdRequestHeaders(hs: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
      hs.filter(h => keep(h, h.renderInRequests(), n => n != "host" && n != "accept-encoding"))

    def fwdResponseHeaders(hs: immutable.Seq[HttpHeader]): immutable.Seq[HttpHeader] =
      hs.filter(h => keep(h, h.renderInResponses(), _ => true))

    def forwardedHeaders(req: HttpRequest): List[HttpHeader] = {
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

    def proxy(req: HttpRequest): Future[HttpResponse] = {
      val withPath = cfg.origin.withPath(req.uri.path)
      val target   = req.uri.rawQueryString.fold(withPath)(withPath.withRawQueryString)
      val outgoing = req.withUri(target).withHeaders(fwdRequestHeaders(req.headers) ++ forwardedHeaders(req))

      Http()
        .singleRequest(outgoing, settings = poolSettings)
        .map(_.withProtocol(req.protocol)) // serve client's protocol, not origin's (1.0+chunked is illegal)
        .map(RewriteHttp.rewriteResponseWith(rewriteFlow, cfg.accept))
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
      val start = System.nanoTime()
      val result =
        if (req.uri.path.toString == cfg.healthPath)
          Future.successful(HttpResponse(StatusCodes.OK, entity = "ok\n"))
        else proxy(req)
      result.map { resp =>
        val ms = (System.nanoTime() - start) / 1000000
        log.info("{} {} -> {} {}ms", req.method.value, req.uri.path, resp.status.intValue, ms)
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

  def main(args: Array[String]): Unit = {
    // Load config: explicit file arg, else standard ConfigFactory (honours -Dconfig.file).
    val config = args.headOption match {
      case Some(path) =>
        ConfigFactory.parseFile(new File(path)).withFallback(ConfigFactory.load()).resolve()
      case None => ConfigFactory.load()
    }
    val cfg = ProxyConfig.from(config.getConfig("prism.proxy"))

    implicit val system: ActorSystem[Nothing] =
      ActorSystem(Behaviors.empty, "prism-proxy", config)
    import system.executionContext
    val log          = org.slf4j.LoggerFactory.getLogger("prism.proxy")
    val poolSettings = ConnectionPoolSettings(system)
    val scheme       = if (cfg.tls.isDefined) "https" else "http"

    val serverAt = Http().newServerAt(cfg.interface, cfg.port)
    val server   = httpsContext(cfg).fold(serverAt)(serverAt.enableHttps)

    server.bind(buildHandler(cfg)).onComplete {
      case Success(binding) =>
        log.info("prism proxy {}://{}:{}/  ->  {}  ({} body + {} header rule(s), pool {}/{})",
          scheme, cfg.interface, cfg.port, cfg.origin,
          cfg.rules.size, cfg.headerRules.size, poolSettings.maxConnections, poolSettings.maxOpenRequests)
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
