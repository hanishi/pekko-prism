package prism

import org.apache.pekko.util.ByteString
import org.openjdk.jmh.annotations.*

import java.util.concurrent.TimeUnit
import java.util.regex.{Matcher, Pattern}
import scala.compiletime.uninitialized

/**
 * Per-message rewriting (Kafka / Pub/Sub style): one complete payload in hand, several
 * literal rules. Compares the alternatives people actually reach for against the engine:
 *
 *   - chained `String.replace` (one pass per pattern, the naive multi-pattern form)
 *   - a compiled regex alternation + matcher (the "smart" single-pass regex)
 *   - LiteralRewriter (Aho-Corasick)
 *   - WuManberRewriter (what the config dispatch picks for independent multi-patterns)
 *
 * All four produce the identical rewritten message (checked in `setup`), so the times
 * are apples-to-apples. AverageTime in microseconds per message.
 *
 *   sbt "bench/Jmh/run -f1 -wi 4 -i 5 .*MessageBench.*"
 */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(2)
class MessageBench {

  private val rules = Seq(
    "internal.example.com" -> "www.example.com",
    "tracker.example.com"  -> "fp.example.com",
    "us-east-1"            -> "eu-west-1",
    "staging.svc"          -> "prod.svc",
    "/v1/api/"             -> "/v2/api/"
  )

  // A "decent" ~4 KB event payload, with the patterns sprinkled through it.
  private val unit =
    """{"event":"pageview","user":"u-93271","url":"https://internal.example.com/v1/api/products/9",""" +
    """"ref":"https://tracker.example.com/r?to=https%3A%2F%2Finternal.example.com","region":"us-east-1",""" +
    """"host":"staging.svc.internal.example.com","ts":1700000000,"ua":"Mozilla/5.0 prod/test"}""" + "\n"

  private val map   = rules.toMap
  private val regex = Pattern.compile(rules.map(r => Pattern.quote(r._1)).mkString("|"))

  // ~4 KB, ~70 KB, ~1.1 MB (the unit is ~280 bytes)
  @Param(Array("16", "256", "4096"))
  var reps: Int = uninitialized

  // Single-pattern variant, to pit Boyer-Moore-Horspool against one `String.replace`.
  private val oneFrom = "internal.example.com"
  private val oneTo   = "www.example.com"

  var msg: String          = uninitialized
  var msgBytes: ByteString = uninitialized
  var ac: Rewriter         = uninitialized
  var wm: Rewriter         = uninitialized
  var bmh: Rewriter        = uninitialized

  @Setup def setup(): Unit = {
    msg      = unit * reps
    msgBytes = ByteString(msg)
    ac       = new LiteralRewriter(rules)
    wm       = new WuManberRewriter(rules)
    bmh      = BmhRewriter(oneFrom, oneTo)
    // the multi-pattern trio must agree; the single-pattern pair must agree
    require(chainedReplace() == regexAlternation(), "replace vs regex differ")
    require(prismAhoCorasick().utf8String == chainedReplace(), "AC vs replace differ")
    require(prismWuManber().utf8String == chainedReplace(), "WM vs replace differ")
    require(prismBmhSingle().utf8String == replaceSingle(), "BMH vs replace differ")
  }

  @Benchmark def chainedReplace(): String =
    rules.foldLeft(msg) { case (s, (f, t)) => s.replace(f, t) }

  @Benchmark def regexAlternation(): String = {
    val m  = regex.matcher(msg)
    val sb = new java.lang.StringBuilder(msg.length)
    while (m.find()) m.appendReplacement(sb, Matcher.quoteReplacement(map(m.group())))
    m.appendTail(sb)
    sb.toString
  }

  @Benchmark def prismAhoCorasick(): ByteString = ac(msgBytes, atEOF = true)._1
  @Benchmark def prismWuManber(): ByteString    = wm(msgBytes, atEOF = true)._1

  // single pattern: one vectorized String.replace vs Boyer-Moore-Horspool
  @Benchmark def replaceSingle(): String     = msg.replace(oneFrom, oneTo)
  @Benchmark def prismBmhSingle(): ByteString = bmh(msgBytes, atEOF = true)._1
}
