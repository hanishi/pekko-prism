# pekko-prism

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

At a Chinese tech company now known everywhere as a giant, its Japanese joint venture
had to sell over a B2B marketplace that had no Japanese localization, on an origin it
could not change. A local systems integrator's first attempt just parsed the whole page
with regular expressions: to a vendor who only knows web development, every problem
looks like web development. It wasn't acceptable. The real problem is harder, and
streaming: rewrite the HTTP body as it flows, correctly across chunk boundaries, without
buffering. Webtide took it on, and Greg Wilkins' answer was **jetty-prism**: a streaming
Jetty proxy that translated English to Japanese on the fly (the translation map partly
in Groovy, even strings inside JavaScript), in production until the site shipped native
localization.

**pekko-prism is a clean-room reimplementation of that idea on Pekko Streams**: the
concept recalled and rebuilt from scratch, with a few upgrades that trace back to where
the original hurt:

| Concern                          | `jetty-prism` (Jetty 7/8 era)           | here (Pekko)                                               |
| -------------------------------- | --------------------------------------- | ---------------------------------------------------------- |
| Rewriting / translation map      | partly Groovy                           | typed Scala, single-pass automaton                         |
| Performance                      | Groovy + translate-everything, slow     | JVM + Aho-Corasick (~210 MB/s/core)                        |
| Text inside `<script>`/`<style>` | translated blindly                      | **optional**: skip with the HTML tokenizer, or rewrite all |
| Multi-pattern match              | Rabin-Karp, length-grouped              | Aho-Corasick (tighter carry bound, linear)                 |
| Chunk carry-over                 | dual `b0`/`b1` buffers, manual indexing | one `carry: ByteString` prepended to the next chunk        |
| "need more bytes"                | `replace()==null` then break            | smaller `consumed`; the tail stays in carry                |
| Flush at end                     | `flush()`                               | `onUpstreamFinish` with `atEOF=true`                       |
| Backpressure                     | hand-rolled `_skip` counter             | free, from Pekko Streams demand                            |

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

The data flows one chunk at a time; the only state is the carry:

```
   origin response                 RewriteFlow                  to the client
   (chunks in)                                                  (chunks out)

      chunk  -->  buf = carry ++ chunk  -->  rewriter(buf, atEOF)
                                             = (output, consumed)  -->  output
                  carry = buf.drop(consumed)
                   `-- the unfinalized tail (< longest pattern), held for next chunk
```

So a pattern split across two chunks is still matched. With `internal -> EXTERNAL` and
the body split `...http://inter` | `nal/x"...`, push 1 emits up to `inter` and carries
it; push 2 prepends the carry, sees `internal`, and emits `EXTERNAL`. The carry never
exceeds the longest pattern, so memory stays bounded no matter how the stream is framed.

### Rewriters

| Rewriter                                       | What it does                                                                                                     |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `LiteralRewriter`                              | multi-pattern literal `from -> to` (whole body)                                                                  |
| `WordLiteralRewriter`                          | same, but only on whole words (`head` is not `header`/`ahead`)                                                   |
| `UrlAttributeRewriter`                         | rewrite only HTML attribute *values* (`href`, `src`, …); case-insensitive, entity-decoding                       |
| `TokenRewriter`                                | **capture** a token (e.g. a URL) and emit `transform(captured)`; the replacement is a function of what was there |
| `HtmlTextRewriteStage` / `HtmlTextRewriteFlow` | apply any inner rewriter **only to HTML text nodes**, never tags, attributes, `<script>`/`<style>`, or comments  |

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

`prism.ProxyServer` is a real Pekko HTTP service whose behavior comes entirely from a
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
./run-proxy-server.sh proxy.conf
```

It also supports **scoped rules** (`when { path/host/method }`, so a rule fires only on
matching requests), **request-side rules** (`request-rules`: set/strip request headers,
rewrite the request body before forwarding), a Prometheus **`/metrics`** endpoint, and
**hot config reload** (`reload = true` swaps rules live, no restart).

The full configuration reference (every setting, all rule types, how rules combine,
and worked recipes) is in [`docs/proxy-config.md`](docs/proxy-config.md).

## Deploy (Docker / Kubernetes)

A prebuilt distroless image is published at `docker.io/hanishi/pekko-prism:latest`, and
`deploy/` already points at it, so a deploy is two steps:

```
# 1. tailor deploy/configmap.yaml: set `origin` and your `rules`
kubectl apply -f deploy/                  # ConfigMap + Deployment + Service
```

The image is generic and carries **no** origin or rules, so it does nothing useful
until you supply them: edit `deploy/configmap.yaml` (mounted at `/config/proxy.conf`)
with your upstream `origin` and the `rules` you want. The proxy is config-driven, so
the image is the same for everyone; only the ConfigMap differs.

To build and publish your **own** image instead of the prebuilt one:

```
sbt assembly                              # -> target/scala-3.3.4/prism-proxy.jar
docker build -t <you>/pekko-prism:latest .
docker push  <you>/pekko-prism:latest     # then set that in deploy/deployment.yaml
```

(For a local-only image, load it into the cluster nodes with `kind load docker-image …`
/ `minikube image load …` instead of pushing, or you get `ImagePullBackOff`.)

The image is **distroless** (`gcr.io/distroless/java21-debian12`): ~237 MB, a real JVM
on a minimal base with no shell or package manager, which cuts the CVE/attack surface.
Because there is no shell, debug with an ephemeral container
(`kubectl debug -it <pod> --image=busybox`), not `kubectl exec -- sh`.

`deploy/` mounts the config as a `ConfigMap` at `/config/proxy.conf`, with
`readiness`/`liveness` probes on `/healthz` and `terminationGracePeriodSeconds: 30`
(longer than the proxy's 10s drain).

**Access it.** The Service is `ClusterIP` (in-cluster only), so reach it with a
port-forward to a **free local port** (one nothing else owns, or the forward silently
loses the bind and you hit the wrong server):

```
kubectl port-forward svc/prism-proxy 8088:80     # -> http://localhost:8088/
```

Pick the local side (`8088`) freely; check it is free with `lsof -i:8088`. For a stable
URL instead of a port-forward, front it with an Ingress, or use a `NodePort`/
`LoadBalancer` Service if your cluster maps those to the host.

**Keep the config's `port = 8080`.** That value is wired to the Deployment's
`containerPort` and the health probes. Change it and the probes hit a dead port and the
pod `CrashLoopBackOff`s. To serve on a different port locally, change only the *local*
side of the port-forward (`8088:80`), never the config.

**Changing config.** With `reload = true` in the config, just `kubectl apply -f deploy/`
and the pods hot-reload (after the kubelet ConfigMap sync, up to ~60s), no restart.
Without it, the proxy reads config only at startup, so `kubectl apply` then
`kubectl rollout restart deploy/prism-proxy`. Turning `reload` on itself needs one
restart to take effect.

**TLS / certificates.** For real certs, terminate TLS at an **Ingress** with
**cert-manager** (Let's Encrypt or your CA) and run the proxy plain HTTP behind it: the
cert is issued and rotated for you, and nothing sensitive sits in the pod. `deploy/` is
wired for this model. To have the proxy terminate TLS itself (the config's `tls {}`
block), mount the PKCS12 keystore from a `Secret` (not the image, not the ConfigMap) and
move the whole config into a `Secret` too, since `tls.password` would otherwise be
plaintext in a ConfigMap.

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

| Benchmark                              | MB/s/core | ns/byte | JMH (µs/op) |
| -------------------------------------- | --------- | ------- | ----------- |
| `LiteralRewriter`                      | ~205      | 4.6     | 4842 ± 56   |
| `WordLiteralRewriter`                  | ~185      | 5.2     | 5450 ± 88   |
| `TokenRewriter` (capture + url-encode) | ~130      | 7.3     | 7628 ± 145  |
| `LiteralRewriter` via `RewriteFlow`    | ~200      | 4.8     | 4986 ± 321  |
| HTML text-node tokenizer (via Flow)    | ~110      | 8.9     | 9360 ± 459  |

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

The key test runs each rewrite at **every possible chunk boundary** (sizes 1..N) and
asserts the output is identical to a single-pass reference.

## Credits

- **Concept:** Greg Wilkins (creator of Jetty, founder of Webtide), who designed the
  original **jetty-prism**, a streaming localization proxy for the Japanese joint
  venture of a now-giant Chinese tech company's B2B marketplace.
- **This reimplementation:** clean-room, from scratch, on Apache Pekko Streams.

## License

Apache License 2.0. See [LICENSE](LICENSE).

Copyright 2026 Haruhiko Nishi.
