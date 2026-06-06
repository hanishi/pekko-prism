package prism.http

import org.apache.pekko.http.scaladsl.coding.{Coder, Coders}
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{`Content-Encoding`, HttpEncodings}
import org.apache.pekko.http.scaladsl.server.Directive0
import org.apache.pekko.http.scaladsl.server.Directives.mapResponse
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.util.ByteString
import prism.{RewriteFlow, Rewriter}

/**
 * Pekko HTTP glue for [[prism.Rewriter]] — the content-safe wrapper around the
 * raw `entity.transformDataBytes(RewriteFlow(...))` call.
 *
 * It closes the three gaps a naive integration hits in production:
 *
 *   1. '''Content-Type gating.''' The rewriter assumes UTF-8 *text*; run it over
 *      a JPEG or a gzipped blob and you corrupt bytes. By default only
 *      `text/html` responses are touched ([[isHtml]]); everything else passes
 *      through untouched. Override `accept` to widen (e.g. include `text/css`).
 *
 *   2. '''Content-Encoding.''' Origins usually reply `Content-Encoding: gzip`;
 *      rewriting compressed bytes is meaningless. We decode gzip/deflate first
 *      (which also strips the now-stale `Content-Encoding` header), rewrite the
 *      plaintext, and serve it identity-encoded. Re-compress at the route with
 *      Pekko's `encodeResponse` directive if you want the wire small again.
 *
 *   3. '''Framing / Content-Length.''' Rewriting changes the body length.
 *      `transformDataBytes` yields a chunked entity whose length Pekko re-derives,
 *      so the framing stays honest — provided you let the entity own it and do
 *      not hand-copy a stale `Content-Length`/`Content-Encoding` header.
 */
object RewriteHttp {

  /** Default gate: only rewrite HTML. */
  val isHtml: ContentType => Boolean = _.mediaType == MediaTypes.`text/html`

  /** True for XML media types, including `+xml` suffixes and VAST's IAB type. */
  def isXml(mt: MediaType): Boolean = {
    val sub = mt.subType
    (mt.mainType == "text" && sub == "xml") ||
    (mt.mainType == "application" && (sub == "xml" || sub.endsWith("+xml")))
  }

  /** Gate for HTML *or* XML (e.g. VAST/RSS/SOAP). */
  val isHtmlOrXml: ContentType => Boolean = ct =>
    ct.mediaType == MediaTypes.`text/html` || isXml(ct.mediaType)

  /** Pick a decoder from the message's `Content-Encoding` (identity ⇒ no-op). */
  private def decoderFor(message: HttpMessage): Coder =
    message
      .header[`Content-Encoding`]
      .flatMap(_.encodings.headOption)
      .getOrElse(HttpEncodings.identity) match {
      case HttpEncodings.gzip    => Coders.Gzip
      case HttpEncodings.deflate => Coders.Deflate
      case _                     => Coders.NoCoding
    }

  /**
   * Rewrite a response body, gated by content type and decompressed first.
   * Non-matching responses are returned unchanged. Use when proxying an origin:
   * `Http().singleRequest(req).map(RewriteHttp.rewriteResponse(rewriter))`.
   */
  def rewriteResponse(
      rewriter: Rewriter,
      accept: ContentType => Boolean = isHtml
  )(resp: HttpResponse): HttpResponse =
    rewriteResponseWith(RewriteFlow(rewriter), accept)(resp)

  /**
   * Like [[rewriteResponse]], but driven by an arbitrary byte-rewriting Flow —
   * use this to chain several rewriters (`RewriteFlow(a) via RewriteFlow(b)`) or
   * to wrap one in the HTML tokenizer.
   */
  def rewriteResponseWith(
      flow: Flow[ByteString, ByteString, ?],
      accept: ContentType => Boolean = isHtml
  )(resp: HttpResponse): HttpResponse =
    if (!accept(resp.entity.contentType)) resp
    else {
      val decoded = decoderFor(resp).decodeMessage(resp) // gunzip + drop Content-Encoding
      decoded.withEntity(decoded.entity.transformDataBytes(flow))
    }

  /**
   * Server-side directive: rewrite every outgoing HTML response of the inner
   * route. `RewriteHttp.rewriteHtmlResponses(rewriter) { ...route... }`.
   */
  def rewriteHtmlResponses(
      rewriter: Rewriter,
      accept: ContentType => Boolean = isHtml
  ): Directive0 =
    mapResponse(rewriteResponse(rewriter, accept))
}
