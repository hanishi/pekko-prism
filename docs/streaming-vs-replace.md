# Streaming rewriting vs `String.replace`

Prism is not meant to replace `String.replace`.

For a complete in-memory string, especially with one literal pattern, `String.replace` is
already excellent. It is simple, heavily optimized, and usually the right tool.

Prism solves a different problem:

> rewriting byte streams correctly while the data is still streaming.

That distinction matters. Once the body is not fully in memory, `.replace` is no longer
just a slower abstraction. It becomes the wrong abstraction.

## The simple case: use `String.replace`

If you already have the whole value in memory:

```scala
val out = input.replace("internal.example.com", "public.example.com")
```

then `String.replace` is hard to beat. For one literal replacement, Prism is not trying to
win; the JDK implementation is highly optimized, and the benchmark reflects that.

Use `String.replace` when all of these are true:

- the full body is already materialized
- the body is small enough to hold comfortably in memory
- the replacement rule is simple
- chunk boundaries do not exist or do not matter

That is not the problem Prism is designed for.

## The streaming problem

HTTP bodies, TCP streams, file streams, and proxy responses do not naturally arrive as one
complete string. They arrive as chunks:

```
Chunk 1: ... href="https://internal.exam
Chunk 2: ple.com/path" ...
```

A per-chunk replacement cannot see the full match, because the pattern crosses the boundary
between two chunks:

```
internal.exam | ple.com
```

A naive implementation like this is incorrect:

```scala
source.map { bytes =>
  ByteString(bytes.utf8String.replace("internal.example.com", "public.example.com"))
}
```

It only rewrites matches that are fully contained inside a single chunk. That means it works
in tests until the stream happens to split at the wrong byte.

Prism is designed for this case. It carries enough boundary state to detect matches that
straddle chunks, without buffering the entire body. (Verified in
`src/test/scala/prism/StreamingVsReplaceSpec.scala`.)

## What Prism provides

Prism is a streaming rewrite engine:

```scala
val flow: Flow[ByteString, ByteString, NotUsed] = RewriteFlow(rewriter)
```

It can be inserted into a Pekko Streams pipeline and used directly on byte streams. The
important properties are:

1. **Chunk-boundary correctness.** Matches are found even when the pattern is split across
   chunks.
2. **Bounded memory.** Prism does not need to buffer the entire body. Memory is bounded by
   the longest pattern and the internal rewrite state, not by the size of the response.
3. **Backpressure.** Prism is a Pekko Streams `Flow`, so it participates in the same
   demand-driven flow control as the rest of the stream. A plain `String.replace` only runs
   after the payload has already been materialized as one complete value; it does not solve
   streaming flow control or bounded-memory rewriting.
4. **Multiple rewrite rules.** Prism is built for rewrite pipelines with multiple patterns,
   host rewrites, tag injection, URL wrapping, and similar proxy transformations.

## Why chained `.replace` changes the cost model

For one literal replacement, this is fine:

```scala
input.replace("a", "b")
```

But multiple replacements usually become chained passes:

```scala
input
  .replace("internal.example.com", "public.example.com")
  .replace("http://", "https://")
  .replace("/old-path", "/new-path")
```

Each replacement scans the string again and may allocate another intermediate string. That
is acceptable for small strings, not ideal for large bodies, and it does not solve streaming
correctness. Chaining can also be *wrong* when one pattern is a substring of another: a later
`.replace` rewrites the output of an earlier one (`a -> X`, then `X -> Y`, turns the original
`a` into `Y`). Prism applies all patterns in a single non-overlapping pass, so replacements
never re-match each other.

## Why this is not just Aho-Corasick

Aho-Corasick is a standard algorithm for matching many literal patterns in one pass. It is
useful, but it is not automatically faster than `String.replace` in every situation.

For complete in-memory strings, HotSpot's `String.indexOf` path is extremely optimized. In
simple literal benchmarks, repeated `replace` calls can be surprisingly competitive, because
the JDK is very good at scanning strings. A generic byte-by-byte state machine can lose to a
highly optimized JDK intrinsic.

Prism's advantage is not merely "one pass." It is the combination of:

- streaming operation
- chunk-boundary correctness
- bounded memory
- backpressure
- rewrite-oriented state handling
- skipping and fast paths where possible

The point is not that every streaming state machine beats `String.replace`. The point is
that `String.replace` is not a streaming rewrite engine.

## What the benchmark actually showed

Measured per complete message (JMH, `bench/src/main/scala/prism/MessageBench.scala`):

- **One literal pattern is a tie.** Boyer-Moore-Horspool and `String.replace` land within
  noise at every size (~478 vs ~516 us on a 1 MB message). Use `.replace`.
- **Several independent patterns: Prism (Wu-Manber) is ~2x faster** than chained `.replace`,
  and **~10x faster than a compiled regex** (1520 vs 2817 vs 14148 us on 1 MB).
- **Aho-Corasick loses to `.replace`** at every size, exactly as described above. Prism's
  speed comes from skipping (BMH / Wu-Manber), not from the automaton.
- The ratios are flat with size (linear scaling), so the picture holds from 4 KB to 1 MB.

The benchmark should be read this way:

| Case                          | Interpretation                                                                |
| ----------------------------- | ----------------------------------------------------------------------------- |
| One literal replacement       | `String.replace` is already excellent. Use it.                                |
| Multiple literal replacements | Chained `.replace` starts paying for repeated scans and intermediate strings. |
| Regex-style rewriting         | General-purpose regex replacement can be much more expensive (~10x here).     |
| Streaming body rewriting      | `.replace` is the wrong abstraction unless the whole body is buffered first.  |
| Chunk-boundary matches        | Per-chunk replacement is incorrect. Prism is designed to handle this.         |

So the benchmark is not saying "Prism is always faster than `String.replace`." The real
claim is: Prism solves the streaming rewrite problem that `String.replace` does not model.

## The core example

Suppose the response contains this URL:

```
https://internal.example.com/docs
```

But the stream arrives like this:

```
Chunk 1: https://internal.exam
Chunk 2: ple.com/docs
```

A per-chunk replacement sees `"https://internal.exam"` and `"ple.com/docs"`. Neither chunk
contains `internal.example.com`, so nothing is rewritten. Prism sees the stream as a
continuous sequence of bytes and keeps enough suffix state to complete the match when the
next chunk arrives. The result is correct:

```
https://public.example.com/docs
```

without buffering the whole response.

## When not to use Prism

Do not use Prism when a simple `String.replace` is enough:

```scala
val result = smallString.replace("foo", "bar")
```

That is simpler, clearer, and likely just as fast or faster. Prism is useful when the
problem has at least one of these properties:

- the data is streaming
- the body may be large
- matches can cross chunk boundaries
- buffering the whole body is undesirable
- rewrite rules are numerous or structured
- the rewrite logic belongs inside a Pekko Streams pipeline
- the transformation is part of a proxy, gateway, crawler, or HTTP middleware

## Summary

`String.replace` is a great string API. Prism is a streaming rewrite engine. Those are
different tools.

For small complete strings, use `String.replace`. For HTTP bodies, proxy responses, TCP
streams, file streams, or any byte stream where matches may cross chunk boundaries, use a
streaming rewriter.

That is the reason Prism exists:

> not to be a faster `.replace`, but to make streaming body rewriting correct, bounded, and
> composable.

## Reproduce

```
sbt "bench/Jmh/run .*MessageBench.*"
```

The benchmark lives in the `bench` subproject, which is never published and is not part of
the library jar.
