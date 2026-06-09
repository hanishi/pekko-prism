# pekko-prism-zio

A proof that the rewriting engine ports off Pekko: the **same** `pekko-prism-core` driven
from **ZIO Streams** instead of Pekko Streams. No HTTP, no `pekko-stream`.

```scala
import prism.{BmhRewriter, ZioRewrite}
import zio.stream.ZStream

ZStream.fromIterable(bytes)
  .via(ZioRewrite.pipeline(BmhRewriter("internal.example.com", "public.example.com")))
```

`ZioRewrite.pipeline(rw): ZPipeline[Any, Nothing, Byte, Byte]` wraps any `Rewriter`
(`LiteralRewriter`, `BmhRewriter`, `WuManberRewriter`, `TokenRewriter`, ...). The carry
logic is identical to the Pekko `RewriteStage`; only the streaming wrapper differs, so it
is chunk-boundary correct in exactly the same way (verified by `ZioRewriteSpec`, including
the every-split oracle).

```
sbt "zio/test"
```

## Why this works

The `Rewriter` contract is framework-agnostic by design:

```scala
(input: ByteString, atEOF: Boolean) => (output: ByteString, consumed: Int)
```

That is the standard streaming-transform shape (the same as Go's `transform.Transformer`
or a `CharsetDecoder`), so it maps directly onto a ZIO `ZChannel`, an fs2 `Pull`, or a
Pekko `GraphStage`. The build reflects this:

```
core  -> matchers + rewriters; only pekko-actor (for ByteString), no streams, no http
root  -> Pekko Streams + Pekko HTTP proxy (dependsOn core)
zio   -> ZIO Streams envelope (dependsOn core), no http
```

The one shared dependency is `ByteString`, a data type from `pekko-actor`. Swapping it for
`zio.Chunk[Byte]` or `scodec.bits.ByteVector` would make `core` fully Pekko-free; left as
`ByteString` here so the algorithm code is untouched and the two envelopes share a type.
