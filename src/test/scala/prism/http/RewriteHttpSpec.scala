package prism.http

import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{`Content-Encoding`, HttpEncodings}
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import prism.LiteralRewriter

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream
import scala.concurrent.Await
import scala.concurrent.duration.*

class RewriteHttpSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("rewrite-http-spec")
  override def afterAll(): Unit = Await.result(system.terminate(), 5.seconds)

  private val rewriter = new LiteralRewriter(Seq("internal" -> "localhost"))

  private def gzip(b: ByteString): ByteString = {
    val bos = new ByteArrayOutputStream()
    val g   = new GZIPOutputStream(bos)
    g.write(b.toArray); g.close()
    ByteString(bos.toByteArray)
  }

  private def bodyOf(resp: HttpResponse): ByteString =
    Await.result(resp.entity.dataBytes.runFold(ByteString.empty)(_ ++ _), 5.seconds)

  "RewriteHttp.rewriteResponse" should {

    "rewrite a gzipped, chunked HTML response and drop Content-Encoding" in {
      val html = """<a href="http://internal/x">go to internal</a>"""
      val gz   = gzip(ByteString(html))
      // deliver the gzip stream as many tiny chunks → exercises the streaming carry
      val entity = HttpEntity.Chunked.fromData(
        ContentTypes.`text/html(UTF-8)`,
        Source(gz.grouped(8).toList)
      )
      val resp = HttpResponse(entity = entity)
        .withHeaders(`Content-Encoding`(HttpEncodings.gzip))

      val out = RewriteHttp.rewriteResponse(rewriter)(resp)

      out.header[`Content-Encoding`] shouldBe None // decoded → header dropped
      bodyOf(out).utf8String shouldBe """<a href="http://localhost/x">go to localhost</a>"""
    }

    "leave non-HTML content untouched (no corruption of binary)" in {
      val gz = gzip(ByteString("internal internal"))
      val resp = HttpResponse(entity = HttpEntity(ContentTypes.`application/octet-stream`, gz))
        .withHeaders(`Content-Encoding`(HttpEncodings.gzip))

      val out = RewriteHttp.rewriteResponse(rewriter)(resp)

      // passed through verbatim: still gzip-encoded, header intact, bytes identical
      out.header[`Content-Encoding`] shouldBe Some(`Content-Encoding`(HttpEncodings.gzip))
      bodyOf(out) shouldBe gz
    }

    "rewrite an identity (uncompressed) HTML response" in {
      val resp = HttpResponse(entity = HttpEntity(ContentTypes.`text/html(UTF-8)`, "see internal here"))
      bodyOf(RewriteHttp.rewriteResponse(rewriter)(resp)).utf8String shouldBe "see localhost here"
    }
  }
}
