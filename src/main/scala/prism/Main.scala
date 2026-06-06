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
 * Self-contained demo:
 *   - an ORIGIN server on :9001 that streams an HTML page in 7-byte chunks
 *     (so patterns are deliberately split across chunk boundaries), and
 *   - a PROXY server on :9000 that fetches the origin and rewrites the body
 *     on the fly with [[RewriteFlow]].
 *
 * Run:  sbt run
 * Then: curl -s http://localhost:9000/   (rewritten)
 *       curl -s http://localhost:9001/   (original)
 */
object Main {

  private val originHtml: String =
    """<html>
      |<head><title>prism demo</title></head>
      |<body>
      |  <a href="http://internal.example.com/page">internal link</a>
      |  <img src="http://internal.example.com/logo.png"/>
      |</body>
      |</html>
      |""".stripMargin

  /** Replace the internal host with the proxy's public host, and inject a tag. */
  private val rewriter = new LiteralRewriter(
    Seq(
      "internal.example.com" -> "localhost:9000",
      "</head>"              -> "<meta name=\"x-rewritten-by\" content=\"prism\"></head>"
    )
  )

  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem[Nothing] =
      ActorSystem(Behaviors.empty, "prism")
    import system.executionContext

    // Re-create the chunked source per request.
    def originSource: Source[ByteString, ?] =
      Source.fromIterator(() => originHtml.getBytes("UTF-8").grouped(7).map(ByteString.fromArray))

    val originRoute =
      path("") {
        get {
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, originSource))
        }
      }

    // Content-safe wrapper: only HTML is touched, gzip/deflate is decoded first,
    // and Pekko re-derives the framing for the rewritten (length-changed) body.
    val proxyRoute =
      get {
        complete {
          Http()
            .singleRequest(HttpRequest(uri = "http://localhost:9001/"))
            .map(RewriteHttp.rewriteResponse(rewriter)): Future[HttpResponse]
        }
      }

    val origin = Http().newServerAt("localhost", 9001).bind(originRoute)
    val proxy  = Http().newServerAt("localhost", 9000).bind(proxyRoute)

    origin.zip(proxy).onComplete {
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
