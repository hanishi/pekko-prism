# `prism.proxy` configuration reference

`ProxyServer` is configured entirely from a HOCON file under the `prism.proxy` key.
This document is the full reference: every setting, every rule type, how rules
combine, what the proxy does to traffic, how to run it, and worked recipes.

- The schema and its defaults live in `src/main/resources/application.conf`.
- Your file only needs to override what differs; everything else falls back to the
  baked-in defaults.
- Config is read once at startup. Changing it requires a restart (or, in Kubernetes,
  `kubectl rollout restart`).

## Contents

1. [Quick start](#quick-start)
2. [Top-level settings](#top-level-settings)
3. [Content-type gate: `accept`](#content-type-gate-accept)
4. [TLS](#tls)
5. [Rules](#rules)
   - [Body rules](#body-rules)
   - [Header rules](#header-rules)
   - [How rules combine](#how-rules-combine)
   - [`text-only` mode](#text-only-mode)
6. [Connection pool to the origin](#connection-pool-to-the-origin)
7. [What the proxy does to traffic](#what-the-proxy-does-to-traffic)
8. [Running it](#running-it)
9. [Recipes](#recipes)
10. [Limitations](#limitations)

## Quick start

```hocon
prism.proxy {
  port   = 8080
  origin = "http://localhost:9001"
  accept = ["html"]
  rules = [
    { type = rewrite, from = "internal.example.com", to = "www.example.com" }
  ]
}
```

```
./run-proxy-server.sh proxy.conf
curl -s http://localhost:8080/
```

## Top-level settings

All keys live under `prism.proxy`.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `interface` | string | `"0.0.0.0"` | Address to bind the server socket to. |
| `port` | int | `8080` | Port to listen on. |
| `origin` | string (URL) | `"http://localhost:9001"` | The upstream this proxy fronts. Scheme may be `http` or `https`. |
| `health-path` | string | `"/healthz"` | Path that returns `200 ok` directly, without proxying. |
| `accept` | list of string | `["html"]` | Which response content types to rewrite. See [below](#content-type-gate-accept). |
| `text-only` | boolean | `false` | Confine body content rewrites to HTML text nodes. See [below](#text-only-mode). |
| `tls` | object | disabled | Serve HTTPS from a PKCS12 keystore. See [below](#tls). |
| `rules` | list of object | `[]` | Body and header rewrite rules, applied in order. See [below](#rules). |

The client connection pool toward the origin is configured under the standard Pekko
key `pekko.http.host-connection-pool`, not under `prism.proxy`. See
[Connection pool](#connection-pool-to-the-origin).

## Content-type gate: `accept`

The proxy only rewrites responses whose `Content-Type` matches `accept`. Everything
else (images, JSON, binaries, already-compressed blobs) passes through untouched, so a
rewrite can never corrupt non-text payloads. Accepted values:

| Value | Matches |
|---|---|
| `"html"` | `text/html` |
| `"xml"` | `text/xml`, `application/xml`, and any `…+xml` (VAST, RSS, SOAP, Atom) |
| `"text/css"` (any `type/subtype`) | exactly that media type |
| `"all"` or `"*"` | every content type (use with care) |

Examples:

```hocon
accept = ["html"]                 # default: HTML only
accept = ["html", "xml"]          # HTML pages and VAST/XML
accept = ["html", "text/css"]     # HTML and stylesheets
accept = ["all"]                  # everything (you own the risk)
```

Gzip/deflate responses are decoded before rewriting and served identity-encoded, so
rules see plaintext regardless of the origin's `Content-Encoding`.

## TLS

Serve HTTPS directly from a PKCS12 keystore. Omit the block (or `enabled = false`) for
plain HTTP, e.g. when an Ingress or a sidecar terminates TLS in front.

```hocon
tls {
  enabled  = true
  keystore = "/etc/prism/proxy.p12"
  password = "changeit"
}
```

Make a self-signed keystore for local testing:

```
keytool -genkeypair -keystore proxy.p12 -storetype PKCS12 -storepass changeit \
  -keyalg RSA -keysize 2048 -validity 365 -alias proxy \
  -dname "CN=localhost" -ext SAN=dns:localhost
```

## Rules

`rules` is an ordered list. Each entry is an object with a `type`. There are two
families: **body** rules rewrite the response body (the byte stream), and **header**
rules rewrite the parsed response headers. They are mixed freely in the same list and
split by type internally.

### Body rules

#### `rewrite`: literal find/replace (whole body)

```hocon
{ type = rewrite, from = "internal.example.com", to = "www.example.com" }
```

| Field | Meaning |
|---|---|
| `from` | literal string to find |
| `to` | replacement |

Non-overlapping, left-to-right, case-sensitive, matched even across chunk boundaries.
Multiple `rewrite` rules are applied together in a single pass.

#### `rewrite-word`: whole-word literal find/replace

```hocon
{ type = rewrite-word, from = "Color", to = "Colour" }
```

Like `rewrite`, but only matches when the pattern is bounded by non-word characters on
both sides, so `head` does not match `header` or `ahead`. Note that `<` and `>` are
word boundaries, so this is not markup-aware (`<head>` still matches); use `text-only`
for markup protection.

#### `wrap-url`: capture a URL and template it

```hocon
{ type = wrap-url
  anchor   = "https://tracker.example.com"
  template = "https://fp.example.com/collect?dest={enc}" }
```

| Field | Meaning |
|---|---|
| `anchor` | string a URL starts with; the captured token runs from here to the next URL delimiter |
| `template` | replacement, with `{url}` = the captured URL and `{enc}` = its URL-encoded form |

The replacement is a function of the captured original, which is what first-party
tracker proxying needs (capture the third-party URL, encode it into a first-party
endpoint). Always whole-body, since URLs live in attributes and CDATA, not text nodes.

#### `insert-before` / `insert-after`: inject markup at an anchor

```hocon
{ type = insert-before, anchor = "</head>", html = "<script src=\"/x.js\"></script>" }
{ type = insert-after,  anchor = "<body>",  html = "<div id=banner>...</div>" }
```

| Field | Meaning |
|---|---|
| `anchor` | literal string to insert relative to (typically a tag) |
| `html` | content inserted immediately before / after each `anchor` |

For schema-significant formats like VAST, insert before the element that should follow
yours (e.g. insert an `<Impression>` before `<Creatives>`), not just before a closing
tag. Insertion is always whole-body and runs last, so injected markup is not itself
rewritten.

### Header rules

Header rules operate on the parsed response (the typed header model), not on the body
text. A body rewrite therefore can never corrupt `Content-Length` / `Set-Cookie`, and
a header rule can never touch the body.

#### `cookie-flags`: set or strip cookie attributes

```hocon
{ type = cookie-flags, http-only = true, secure = true, same-site = "Lax" }
```

| Field | Type | Meaning |
|---|---|---|
| `http-only` | boolean (optional) | set `HttpOnly` to this value on every `Set-Cookie` |
| `secure` | boolean (optional) | set `Secure` |
| `same-site` | `"Lax"` / `"Strict"` / `"None"` (optional) | set `SameSite` |

Omitted fields are left unchanged. `true` enforces a flag (a deployable hardening for
an app you cannot change). `http-only = false` strips it, which weakens security and
is for authorized testing only, never production user traffic.

#### `set-header`: add or replace a header

```hocon
{ type = set-header, name = "Content-Security-Policy", value = "default-src 'self'" }
```

Removes any existing header of the same name, then adds it. Use for security headers
(CSP, HSTS, `X-Frame-Options`). Do not use it for entity headers like `Content-Type`
or `Content-Length`, which the entity owns.

#### `strip-header`: remove a header

```hocon
{ type = strip-header, name = "X-Powered-By" }
```

Removes every header of that name (case-insensitive). Use to drop fingerprinting
headers like `Server` / `X-Powered-By`.

### How rules combine

Within a response the proxy applies, in this order:

1. **Body content rewrites**: all `rewrite` rules together, then all `rewrite-word`
   rules, each as one pass (wrapped by the tokenizer if `text-only`).
2. **`wrap-url`** rewrites, each as its own pass.
3. **`insert-before` / `insert-after`**, together, last.
4. **Header rules**, in the order written, after the body is rewritten and re-framed.

Body rewrites do not match across each other's output (replacements are not
re-scanned), and inserts run after content rewrites so injected markup is left alone.

### `text-only` mode

```hocon
text-only = true
```

When set, `rewrite` and `rewrite-word` are confined to HTML **text nodes**: tags,
attributes, `<script>`, `<style>`, and comments are skipped. This makes even common
words safe to rewrite (`<head>`, `class="header"`, `<script>head</script>` are left
alone while the word `head` in visible text is rewritten). `wrap-url` and the `insert`
rules are always whole-body and are unaffected by `text-only`.

The tokenizer is a fast, pragmatic HTML scanner, not a full HTML5 parser. See
[Limitations](#limitations).

## Connection pool to the origin

Tuned with the standard Pekko key (not under `prism.proxy`). Pekko's stock defaults
(`max-connections = 4`, `max-open-requests = 32`) are the classic proxy bottleneck
under load; prism ships saner defaults, overridable per deployment.

```hocon
pekko.http.host-connection-pool {
  max-connections   = 64    # simultaneous TCP connections to the origin
  max-open-requests = 256   # in-flight requests buffered; power of two; >= max-connections
  min-connections   = 0     # warm connections kept open
  idle-timeout      = 30 s  # close a pooled connection after this much inactivity
}
```

Sizing: `max-connections` is roughly `peak_RPS * origin_latency_seconds`;
`max-open-requests` must be a power of two and at least your peak concurrency, or
overflow requests are rejected (fast 500s that masquerade as high throughput).

## What the proxy does to traffic

On every request the proxy:

- Answers `health-path` with `200 ok` and no upstream call.
- Forwards the request to `origin`, preserving method, path, query, and body.
- Drops hop-by-hop and synthetic headers, and rewrites `Host` to the origin.
- Adds `X-Forwarded-For` (real client IP), `X-Forwarded-Proto`, `X-Forwarded-Host`,
  and `Via: 1.1 prism`.
- Applies the configured rules to the response (gated by `accept`).
- Serves the response on the **client's** HTTP version, so a rewritten (chunked) body
  is never paired with a stale `HTTP/1.0` from the origin.
- On upstream failure returns `502 Bad Gateway` (or `504 Gateway Timeout` on timeout),
  not a leaked stack trace.
- Logs one access line per request: `GET /path -> 200 12ms` (the `prism` logger at
  `INFO`).
- On `SIGTERM` stops accepting, drains in-flight requests (10s), then exits.

## Running it

```
# sbt
./run-proxy-server.sh proxy.conf
java -Dconfig.file=proxy.conf -cp <classpath> prism.ProxyServer

# fat jar
sbt assembly
java -jar target/scala-3.3.4/prism-proxy.jar proxy.conf
java -jar target/scala-3.3.4/prism-proxy.jar            # no arg: application.conf defaults

# docker (config injected, not baked)
docker build -t prism-proxy:latest .
docker run --rm -p 8080:8080 -v "$PWD/proxy.conf:/config/proxy.conf" \
  prism-proxy:latest /config/proxy.conf

# kubernetes
kubectl apply -f deploy/     # ConfigMap (config) + Deployment + Service
```

A config file passed as the first argument is layered on top of `application.conf`
(so you only specify overrides). With no argument, the built-in defaults are used.

## Recipes

**Localization / content translation** (the founding use case):

```hocon
text-only = true
rules = [ { type = rewrite, from = "Sign in", to = "ログイン" } ]
```

**Brand an asset host behind your CDN:**

```hocon
rules = [ { type = rewrite, from = "assets.vendor.com", to = "cdn.example.com" } ]
```

**Inject analytics before `</head>`:**

```hocon
rules = [ { type = insert-before, anchor = "</head>",
            html = "<script src=\"/analytics.js\"></script>" } ]
```

**First-party tracker proxying** (capture the original, encode it first-party):

```hocon
rules = [ { type = wrap-url
            anchor   = "https://tracker.example.com"
            template = "https://fp.example.com/collect?dest={enc}" } ]
```

**VAST: rewrite trackers, substitute a macro, add an Impression:**

```hocon
accept = ["html", "xml"]
rules = [
  { type = rewrite,        from = "tracker.example.com", to = "fp.example.com" }
  { type = rewrite,        from = "[CACHEBUSTING]", to = "1700000000" }
  { type = insert-before,  anchor = "<Creatives>"
    html = "<Impression><![CDATA[https://fp.example.com/imp]]></Impression>" }
]
```

**Cookie / header hardening in front of a legacy app:**

```hocon
rules = [
  { type = cookie-flags, http-only = true, secure = true, same-site = "Lax" }
  { type = set-header,   name = "Content-Security-Policy", value = "default-src 'self'" }
  { type = set-header,   name = "Strict-Transport-Security", value = "max-age=31536000" }
  { type = strip-header, name = "Server" }
  { type = strip-header, name = "X-Powered-By" }
]
```

## Limitations

- `text-only` uses a fast HTML scanner (tags with quoted attributes, comments,
  `<script>` / `<style>` raw text). It is not a full HTML5 parser: no CDATA-in-HTML,
  no RCDATA `<title>` / `<textarea>`, no encoding detection. Unrecognized constructs
  are emitted verbatim (safe).
- `rewrite` / `rewrite-word` are byte-level with a fixed UTF-8 charset; they do not
  transcode.
- `rewrite-word` is `\b`-style word matching, not markup awareness. Use `text-only`
  to protect markup.
- Config is read at startup; there is no hot reload.
- The proxy rewrites responses. Request-body rewriting and serving inbound first-party
  endpoints (e.g. a `/collect` that forwards captured hits) are not built in.
