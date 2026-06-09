---
name: prism-config
description: Generate a tailored prism.proxy config for a target website. Fetch the site, inspect its hosts, links, trackers, response headers, cookies, and CSP, then emit a pekko-prism proxy.conf with sensible rewrite / header / request rules. Use when someone wants a proxy config for a specific site they want to front and rewrite, e.g. "make a prism config for https://example.com".
allowed-tools: Bash, WebFetch, Read, Write
---

# Generate a `prism.proxy` config for a target website

You produce a valid, tailored `prism.proxy` HOCON config for **pekko-prism** by actually
**inspecting the target site**, not by guessing. The useful rules depend on what the site
really serves (relative vs absolute links, which trackers, which headers), so always look
first.

`docs/proxy-config.md` in this repo is the authoritative schema. `application.conf` has the
defaults. Consult them if unsure; the cheat sheet below covers the common cases.

## Inputs

- **target URL** (required): the site to front and rewrite, e.g. `https://example.com`.
- **public/proxy host** (optional): the hostname users will hit the proxy on. Needed only
  if you intend to repoint the origin's absolute URLs back through the proxy. Ask if the
  user wants host rewriting and didn't say.
- **goal** (optional): localize/translate, front-and-harden, first-party tracker proxying,
  tag injection, etc. If unstated, infer from what the site exposes and confirm.

If the target URL is missing, ask for it. Everything else can be inferred and confirmed.

## Step 1 — Inspect the target (do this first, every time)

Fetch headers and a sample of the HTML, and extract concrete signals:

```bash
URL="https://TARGET"
curl -sIL "$URL" | grep -iE "^(server|x-powered-by|content-type|set-cookie|content-security-policy|strict-transport-security|x-frame-options|content-encoding)"
curl -sL "$URL" | head -c 200000 > /tmp/prism_target.html
# absolute URLs and the hosts they point at (link/asset/tracker candidates)
grep -oiE 'https?://[a-z0-9.-]+' /tmp/prism_target.html | sort | uniq -c | sort -rn | head -40
```

(Or use WebFetch on the URL for a higher-level read of structure and third-party scripts.)

From that, determine:

- **Origin host** = the target's hostname.
- **Links relative or absolute?** If the page uses relative links (`/path`), host rewriting
  is a **no-op** -- do NOT generate a host-rewrite rule. Only generate one if the page emits
  **absolute** URLs pointing back at the origin host and the user wants visitors to stay on
  the proxy.
- **Third-party hosts**: trackers/analytics/pixels (google-analytics, googletagmanager,
  doubleclick, facebook, hotjar, segment, etc.), and asset CDNs.
- **Response headers**: `Server` / `X-Powered-By` (strip candidates), `Set-Cookie` flags
  (harden candidates), `Content-Security-Policy` (note if it would block proxied/rewritten
  assets), missing security headers.
- **Content-Type**: drives `accept` (html; add xml for feeds/VAST/RSS/SOAP).

Report what you found in a short list before proposing rules.

## Step 2 — Map findings to rules

| Found on the target | Rule to generate |
|---|---|
| absolute URLs to the origin host, want users on the proxy | `{ type = rewrite, from = "origin.host", to = "proxy.host" }` (or per-attribute via the engine's `UrlAttributeRewriter` if only `href`/`src`) |
| third-party tracker/pixel URL you want first-party | `{ type = wrap-url, anchor = "https://tracker.host", template = "https://PROXY/collect?dest={enc}" }` |
| asset CDN you want to repoint | `{ type = rewrite, from = "assets.host", to = "cdn.yours" }` (mind CSP, below) |
| `Server` / `X-Powered-By` present | `{ type = strip-header, name = "Server" }` etc. |
| weak `Set-Cookie` (no HttpOnly/Secure/SameSite) | `{ type = cookie-flags, http-only = true, secure = true, same-site = "Lax" }` |
| missing security headers | `{ type = set-header, name = "Content-Security-Policy", value = "..." }`, HSTS, `X-Frame-Options` |
| inject analytics/CMP/meta | `{ type = insert-before, anchor = "</head>", html = "..." }` |
| brand text to localize/translate | `{ type = rewrite, from = "Sign in", to = "..." }` or `rewrite-word` for whole words |
| want text rewrites without touching markup | top-level `text-only = true` |
| scope a rule to part of the site | add `when { path = "/api/*" }` / `host` / `method` to the rule |
| request-side (add an upstream auth header, etc.) | `request-rules = [ { type = set-header, name = "...", value = "..." } ]` |

## Step 3 — Emit the config

Top-level keys under `prism.proxy` (defaults in parentheses):

`interface ("0.0.0.0")`, `port (8080)`, `origin` (required), `health-path ("/healthz")`,
`metrics-path ("/metrics")`, `reload (false)`, `handle-options (false)`,
`accept (["html"])`, `text-only (false)`, `tls { enabled keystore password }`, `rules []`,
`request-rules []`. The client pool is the standard
`pekko.http.host-connection-pool { max-connections, max-open-requests }`, outside
`prism.proxy`.

Produce a clean, commented config, for example:

```hocon
prism.proxy {
  port   = 8080
  origin = "https://target.example.com"
  accept = ["html"]
  reload = true
  handle-options = true
  rules = [
    { type = rewrite,      from = "target.example.com", to = "proxy.example.com" }
    { type = strip-header, name = "Server" }
    { type = cookie-flags, http-only = true, secure = true, same-site = "Lax" }
  ]
}
pekko.http.host-connection-pool { max-connections = 128, max-open-requests = 512 }
```

## Gotchas to honor (do not violate these)

- **Keep `port = 8080`.** It is wired to the k8s `containerPort` and health probes; change
  it and pods CrashLoopBackOff. To serve on another port locally, change the
  `kubectl port-forward` local side, not the config.
- **Do not invent a host-rewrite rule for a relative-link site** -- it does nothing. Only
  add it when the page actually emits absolute URLs to the origin.
- **`Content-Security-Policy: default-src 'self'` will break a proxied third-party site**
  (its assets live on the origin's domain, which `'self'` blocks). If you add/keep a CSP,
  widen it to the asset origins or rewrite the asset URLs first.
- **Use real placeholders, not made-up domains** for things the user must fill in
  (`PROXY_HOST`, keystore path, etc.), and say so.
- Suggest `reload = true` and `handle-options = true` by default for a live proxy; mention
  `text-only = true` when the site has common words you'd rewrite that also appear in markup.
- Informational only: the engine auto-picks the matcher (1 literal rule -> Boyer-Moore-Horspool,
  several independent -> Wu-Manber, else Aho-Corasick). The author does not choose it.

## Step 4 — Deliver

- Print the config, with a one-line note per non-obvious rule explaining why it's there
  (tie each back to what you found on the site).
- Offer to write it to `proxy.conf`, or into `deploy/configmap.yaml`'s `proxy.conf` block.
- Tell them how to run and verify:

  ```
  ./run-proxy-server.sh proxy.conf
  curl -s http://localhost:8080/        # see the rewritten page
  ```

Be honest: if the target's links are relative and it has no trackers or weak headers, say
so and generate a minimal config (origin + accept) rather than padding it with useless rules.
