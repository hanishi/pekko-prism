# Streaming rewriting vs `String.replace` / regex

When does this engine actually beat a plain `msg.replace(a, b)` or a regex? We measured
it (JMH, `bench/src/main/scala/prism/MessageBench.scala`) on complete in-memory messages,
then drew the line where streaming makes the alternatives not just slower but impossible.

## Complete messages: the honest scorecard

These are complete payloads already in memory (a Kafka record, a Pub/Sub message, an HTTP
body you've buffered). All methods produce byte-identical output (checked in the bench).

Microseconds per message, **multiple patterns** (5 independent literal rules):

| size    | regex | Aho-Corasick | chained `.replace` | Wu-Manber |
| ------- | ----- | ------------ | ------------------ | --------- |
| ~4.5 KB | 55.3  | 21.4         | 10.7               | **5.3**   |
| ~72 KB  | 861   | 312          | 193                | **90**    |
| ~1.1 MB | 14148 | 5053         | 2817               | **1520**  |

Microseconds per message, **single pattern**:

| size    | `String.replace` | Boyer-Moore-Horspool |
| ------- | ---------------- | -------------------- |
| ~4.5 KB | 1.94             | 1.87                 |
| ~72 KB  | 30.9             | 30.7                 |
| ~1.1 MB | 516              | 478                  |

What the numbers say:

- **One literal pattern is a tie.** BMH and `String.replace` trade blows (a scalar skip
  vs a vectorized `indexOf` intrinsic). The engine gives no speed edge here; `.replace`
  is simpler, so use it.
- **Multiple patterns: the engine wins ~2x over chained `.replace`** (which needs one
  pass per pattern) and **~10x over regex**. Regex is the worst by a wide margin at every
  size.
- **Aho-Corasick loses to `.replace`.** A serial state machine that touches every byte
  cannot beat a SIMD `indexOf`, even across 5 passes. The engine's speed comes from
  *skipping* (BMH / Wu-Manber), which is exactly why the matcher dispatch reaches for
  those first and treats Aho-Corasick as the correctness floor.
- **The ratios are flat with size** (linear scaling), so this picture holds from 4 KB to
  1 MB; only the absolute savings grow.

Two things the timings do not show, but matter:

- For patterns where one **contains** another, chained `.replace` is *wrong*: it cascades
  (`a -> X` then `X -> Y` double-rewrites). The engine (Aho-Corasick) is correct.
- The engine does rewrites `.replace` cannot express at all (whole-word,
  capture-and-transform, attribute-scoped, HTML-text-only); the only `.replace`
  alternative for those is a regex.

## The line `.replace` cannot cross: streaming

Everything above assumes the **whole message is in memory**. The moment it is not,
`String.replace` stops being slower and becomes impossible:

1. **It needs the entire body at once.** A 1 GB response, or an unbounded stream, must be
   fully buffered before `.replace` can run. The engine processes it in fixed-size chunks
   with memory bounded by the longest pattern, not by the body.

2. **Per-chunk `.replace` silently misses matches that straddle a chunk boundary.** If you
   try to "stream" by `.replace`-ing each chunk independently, a pattern split across two
   chunks is never found:

   ```
   chunk 1: ...href="http://internal.examp
   chunk 2: le.com/x"...

   per-chunk .replace("internal.example.com", "X"):
      chunk 1 -> unchanged (no full match)
      chunk 2 -> unchanged (no full match)
      joined  -> "...internal.example.com/x..."   # MISSED, still there

   streaming engine (carry):
      chunk 1 -> emit up to "internal.examp", hold it as carry
      chunk 2 -> prepend carry, see "internal.example.com", emit "X"
      joined  -> "...X/x..."                       # caught
   ```

   This is the whole reason the engine exists: the carry stitches matches across arbitrary
   network/chunk boundaries, which a stateless `.replace` over independent chunks cannot
   do. It is verified by `src/test/scala/prism/StreamingVsReplaceSpec.scala` and by the
   every-chunk-boundary oracle across the rewriter specs.

3. **Backpressure.** As a Pekko Streams `Flow`, it inherits flow control; a `.replace`
   pipeline has none.

## When to use what

| situation                                | use                            |
| ---------------------------------------- | ------------------------------ |
| one literal swap, small in-memory message| `String.replace`               |
| several patterns, in-memory message      | this engine (~2x, and correct) |
| word / capture / attribute / HTML-aware  | this engine (regex otherwise)  |
| streaming / chunked / too big to buffer  | **this engine only**           |
| anything you would write a regex for     | this engine (~10x, saner)      |

## Reproduce

```
sbt "bench/Jmh/run .*MessageBench.*"
```

The benchmark lives in the `bench` subproject, which is never published and is not part of
the library jar.
