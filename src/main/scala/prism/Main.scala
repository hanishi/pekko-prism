package prism

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.server.Directives.*
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import prism.http.RewriteHttp

import scala.concurrent.Future
import scala.io.StdIn
import scala.util.{Failure, Success}

/**
 * Self-contained demo of rewriting across chunk boundaries:
 *
 *   - an ORIGIN on :9001 that streams an HTML page in 7-byte chunks (so the
 *     patterns are deliberately split across chunks), and
 *   - a PROXY on :9000 that fetches the origin and rewrites the body on the fly
 *     with [[prism.http.RewriteHttp]].
 *
 * Run: `sbt run`, then
 *   curl -s http://localhost:9001/   # original
 *   curl -s http://localhost:9000/   # rewritten (host swapped, <meta> injected)
 */
object Main {

  private val OriginUrl = "http://localhost:9001/"

  private val originHtml =
    """<html>
      |<head><title>prism demo</title></head>
      |<body>
      |  <a href="http://internal.example.com/page">internal link</a>
      |  <img src="http://internal.example.com/logo.png"/>
      |</body>
      |</html>
      |""".stripMargin

  /** Swap the internal host for the proxy's host, and inject a <meta> before </head>. */
  private val rewriter = new LiteralRewriter(
    Seq(
      "internal.example.com" -> "localhost:9000",
      "</head>"              -> """<meta name="x-rewritten-by" content="prism"></head>"""
    )
  )

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] = ActorSystem(Behaviors.empty, "prism")
    import system.executionContext

    // A fresh 7-byte-chunked source per request.
    def originSource: Source[ByteString, ?] =
      Source.fromIterator(() => originHtml.getBytes("UTF-8").grouped(7).map(ByteString.fromArray))

    val originRoute = get {
      complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, originSource))
    }

    // Content-safe: only HTML is touched, gzip is decoded first, framing re-derived.
    val proxyRoute = get {
      complete {
        Http()
          .singleRequest(HttpRequest(uri = OriginUrl))
          .map(RewriteHttp.rewriteResponse(rewriter)): Future[HttpResponse]
      }
    }

    val started =
      Http().newServerAt("localhost", 9001).bind(originRoute)
        .zip(Http().newServerAt("localhost", 9000).bind(proxyRoute))

    started.onComplete {
      case Success(_) =>
        println("origin: http://localhost:9001/   proxy: http://localhost:9000/")
        println("Press ENTER to stop.")
      case Failure(e) =>
        System.err.println(s"bind failed: ${e.getMessage}")
        system.terminate()
    }

    StdIn.readLine()
    system.terminate()
  }
}
