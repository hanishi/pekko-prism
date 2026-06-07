# prism-stream

**PRISM**: *PRoxy Integrated Service Module*.

Streaming, chunk-boundary-aware **content rewriting for Apache Pekko**: a modern,
clean-room reimplementation of **jetty-prism**. (Like a prism refracts a beam, it
splits and transforms a byte stream as it passes through.)

If you're on Pekko (or Akka) HTTP and need to rewrite HTTP bodies as they stream
(translate text, rewrite links/hosts, inject tags, capture-and-wrap URLs) correctly
even when a match straddles a chunk boundary, and without buffering the whole body,
the entire engine is one value:

```scala
val flow: Flow[ByteString, ByteString, NotUsed] = RewriteFlow(rewriter)
```

Drop it into any byte stream (an HTTP entity, a TCP connection, a file pipe), and
matches are found and replaced even across chunk boundaries. Backpressure is
inherited from the stream (no manual flow control); memory is bounded by the longest
pattern, regardless of body size.

## Where it comes from

The concept is **Greg Wilkins'**, the creator of Jetty, founder of Webtide.

At a large B2B marketplace, the site had no Japanese localization, yet memberships had
to be sold to Japanese companies. We needed to present a localized experience over an
origin we couldn't change. We took the problem to Webtide, and Greg Wilkins'
answer was **jetty-prism**: a streaming proxy, built on Jetty, that rewrote the page
as it streamed through, translating English to Japanese on the fly. The translation
map and texts were implemented partly in Groovy, and even strings found inside
JavaScript were translated. It cost a lot of performance, but it worked, and it ran
in production until the site shipped native Japanese localization.

**prism-stream is a clean-room reimplementation of that idea on Pekko Streams**: the
concept recalled and rewritten from scratch. Same core: find/replace/inject content
in a byte stream, even when matches cross chunk boundaries, with backpressure and
bounded memory. Different substrate, and a few deliberate upgrades that trace
directly back to where the original hurt:

| Concern | `jetty-prism` (Jetty 7/8 era) | here (Pekko) |
|---|---|---|
| Rewriting / translation map | partly Groovy | typed Scala, single-pass automaton |
| Performance | Groovy + translate-everything, slow | JVM + Aho-Corasick (~210 MB/s/core) |
| Text inside `<script>`/`<style>` | translated blindly | **optional**: skip with the HTML tokenizer, or rewrite all |
| Multi-pattern match | Rabin-Karp, length-grouped | Aho-Corasick (tighter carry bound, linear) |
| Chunk carry-over | dual `b0`/`b1` buffers, manual indexing | one `carry: ByteString` prepended to the next chunk |
| "need more bytes" | `replace()==null` then break | smaller `consumed`; the tail stays in carry |
| Flush at end | `flush()` | `onUpstreamFinish` with `atEOF=true` |
| Backpressure | hand-rolled `_skip` counter | free, from Pekko Streams demand |

> Credit for the original concept: **Greg Wilkins / Webtide**. This implementation
> shares none of the original code; it was rebuilt from the concept.

## Design

A `Rewriter` is a stateless function `(input, atEOF) => (output, consumed)`: emit the
bytes safe to release now, and report how many leading bytes are finalized. Anything
that could still grow into a match is left unconsumed; the streaming envelope keeps
it as the "carry" and prepends it to the next chunk. `RewriteStage` is the `GraphStage`
that manages that carry and flushes on completion; `RewriteFlow` lifts a `Rewriter`
into a reusable `Flow`. The matcher is pluggable; the streaming envelope is
matcher-agnostic.

### Rewriters

| Rewriter | What it does |
|---|---|
| `LiteralRewriter` | multi-pattern literal `from -> to` (whole body) |
| `WordLiteralRewriter` | same, but only on whole words (`head` is not `header`/`ahead`) |
| `UrlAttributeRewriter` | rewrite only HTML attribute *values* (`href`, `src`, …); case-insensitive, entity-decoding |
| `TokenRewriter` | **capture** a token (e.g. a URL) and emit `transform(captured)`; the replacement is a function of what was there |
| `HtmlTextRewriteStage` / `HtmlTextRewriteFlow` | apply any inner rewriter **only to HTML text nodes**, never tags, attributes, `<script>`/`<style>`, or comments |

The carry the streaming envelope retains never exceeds the longest pattern (+1 for
the word/capture variants), so memory is bounded regardless of body size. The key
test runs each rewrite at **every possible chunk boundary** (sizes 1..N) and asserts
the output is identical to a single-pass reference.

## Use it from Pekko HTTP

`prism.http.RewriteHttp` is the content-safe wrapper around `transformDataBytes`: it
gates on `Content-Type` (HTML by default, `isHtmlOrXml` for VAST/RSS/SOAP), decodes
`Content-Encoding` (gzip/deflate) before rewriting, and lets the entity re-derive
framing/`Content-Length`.

```scala
// Server-side: rewrite your own HTML responses
RewriteHttp.rewriteHtmlResponses(rewriter) { myRoute }

// Reverse proxy: rewrite an origin's response on the way back
Http().singleRequest(req).map(RewriteHttp.rewriteResponse(rewriter))
```

## Run it as a config-driven proxy

`prism.ProxyServer` is a real Pekko HTTP service whose behaviour comes entirely from a
HOCON file (`prism.proxy`), no CLI flags. It adds the production niceties: a health
endpoint, `X-Forwarded-*`/`Via` headers, `502`/`504` on upstream failure, per-request
access logs, connection-pool tuning, optional HTTPS, and graceful drain on SIGTERM.

`rules` mixes **body** rules (rewrite the response body) and **header** rules (rewrite
the parsed response headers, not the body text):

```hocon
prism.proxy {
  port   = 8080
  origin = "http://localhost:9001"
  accept = ["html", "xml"]
  rules = [
    # body
    { type = rewrite,       from = "internal.example.com", to = "www.example.com" }
    { type = rewrite-word,  from = "Color", to = "Colour" }
    { type = wrap-url,      anchor = "https://tracker.example.com",
                            template = "https://fp.example.com/collect?dest={enc}" }
    { type = insert-before, anchor = "</head>", html = "<script src=\"/x.js\"></script>" }
    # header
    { type = cookie-flags,  http-only = true, secure = true, same-site = "Lax" }
    { type = set-header,    name = "Content-Security-Policy", value = "default-src 'self'" }
    { type = strip-header,  name = "X-Powered-By" }
  ]
}
pekko.http.host-connection-pool { max-connections = 128, max-open-requests = 512 }
```

Header rules work on the parsed model (`HttpCookie.withHttpOnly(...)`), not on body
text, so a body rewrite can never corrupt `Content-Length`/`Set-Cookie` and vice
versa. `cookie-flags` enforces flags (a deployable hardening); `http-only = false`
strips them, which is for authorized security testing only, never production traffic.

```
./run-proxy-server.sh proxy.conf          # or: java -Dconfig.file=proxy.conf -cp <cp> prism.ProxyServer
```

`ReverseProxy` is a flag-driven CLI variant of the same, handy for quick experiments.

## Deploy (Docker / Kubernetes)

A fat jar + container, ready for K8s: the proxy already exposes `/healthz`, drains
in-flight requests on SIGTERM, logs to stdout, and reads its config from a file.

```
sbt assembly                              # -> target/scala-3.3.4/prism-proxy.jar
docker build -t prism-proxy:latest .
kubectl apply -f deploy/                  # ConfigMap + Deployment + Service
```

`deploy/` wires the config in as a `ConfigMap` (mounted at `/config/proxy.conf`), with
`readiness`/`liveness` probes on `/healthz` and `terminationGracePeriodSeconds: 30`
(longer than the proxy's 10s drain). Front it with an Ingress to terminate TLS, or set
`--tls` / the config's `tls {}` block to serve HTTPS directly. Config changes need a
`kubectl rollout restart` (the proxy reads config at startup).

## What it's good for

- **Localization** (the founding use case): translate/inject content into a site you
  don't control, streaming, at the proxy.
- **Link / host rewriting**: point an origin's URLs at your public domain or CDN
  (`UrlAttributeRewriter`, `LiteralRewriter`).
- **Tag / content injection**: analytics, CMP, banners, meta (`insert-before/after`).
- **Capture-and-wrap**: first-party proxying of tracker/measurement URLs, VAST
  element/macro rewriting (`TokenRewriter`, `--xml`). The original `jetty-prism`
  translated; the same primitive proxies, wraps, and injects.
- **Header / cookie hardening**: enforce `HttpOnly`/`Secure`/`SameSite`, inject CSP /
  HSTS, strip `Server`/`X-Powered-By` in front of an app you can't change
  (`cookie-flags`, `set-header`, `strip-header`).

## Performance

JMH (forked JVM, warmed up, average time with confidence intervals), single core of
a 10-core Apple Silicon machine, JDK 21, driving a ~1 MB body in 8 KB chunks:

| Benchmark | MB/s/core | ns/byte | JMH (µs/op) |
|---|---|---|---|
| `LiteralRewriter` | ~205 | 4.6 | 4842 ± 56 |
| `WordLiteralRewriter` | ~185 | 5.2 | 5450 ± 88 |
| `TokenRewriter` (capture + url-encode) | ~130 | 7.3 | 7628 ± 145 |
| `LiteralRewriter` via `RewriteFlow` | ~200 | 4.8 | 4986 ± 321 |
| HTML text-node tokenizer (via Flow) | ~110 | 8.9 | 9360 ± 459 |

The `RewriteFlow` figure tracks the raw `apply` figure, so Pekko Streams adds
essentially no overhead. O(n) time, **constant memory** (no full-body buffering). For
per-request rewriting this is far below network/origin latency (a 100 KB page rewrites
in well under a millisecond), so the engine is never the bottleneck; throughput scales
across cores.

> Reproduce: `sbt "bench/Jmh/run"`. The benchmarks live in the `bench` subproject,
> which depends on the library but is never published and is not aggregated by root,
> so it stays out of the library jar.

## Scope & honest limitations

- The HTML tokenizer is a fast, pragmatic scanner: tags (with quoted attributes that
  may contain `>`), comments, and `<script>`/`<style>` raw text. It is **not** a
  full HTML5 parser (no CDATA-in-HTML, RCDATA `<title>`/`<textarea>`, or encoding
  detection); unhandled constructs are emitted verbatim (safe).
- `WordLiteralRewriter` is `\b`-style word matching, not markup awareness: it won't
  touch `header`/`ahead`, but `<head>` still matches (`<`/`>` are word boundaries).
  Use the tokenizer for markup protection.
- Rewriters operate on bytes with a configured charset (UTF-8 by default); they do not
  transcode.

## Run the tests

```
sbt test
```

## Credits

- **Concept:** Greg Wilkins (creator of Jetty, founder of Webtide), who designed the
  original **jetty-prism**, a streaming localization proxy for a large B2B marketplace.
- **This reimplementation:** clean-room, from scratch, on Apache Pekko Streams.
