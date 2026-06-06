package prism

import scala.collection.mutable

/**
 * Aho-Corasick multi-pattern matcher over raw bytes.
 *
 * Built once from a set of byte patterns, then driven one byte at a time. The
 * transition table is a full DFA (fail links are pre-resolved into `next`), so
 * [[step]] is O(1) and never loops.
 *
 * For each automaton state we expose two lengths:
 *   - [[depthAt]]  the length of the longest pattern *prefix* that is a suffix
 *                  of the bytes consumed so far. This is what a streaming
 *                  rewriter must hold back, because those trailing bytes could
 *                  still grow into a full match in the next chunk.
 *   - [[matchLenAt]] the length of the longest *complete* pattern ending at the
 *                  current position (0 if none), with [[matchIdAt]] giving which
 *                  pattern matched.
 */
final class AhoCorasick private (
    private val next: Array[Array[Int]], // next(state)(byte 0..255) -> state (total DFA)
    private val depth: Array[Int],       // trie depth of each state == longest prefix-suffix length
    private val outLen: Array[Int],      // longest complete pattern ending at state, 0 if none
    private val outId: Array[Int],       // id of that pattern, -1 if none
    val maxPatternLength: Int
) {
  def root: Int = 0

  /** Advance the automaton by one byte and return the new state. */
  def step(state: Int, b: Byte): Int = next(state)(b & 0xff)

  /** Longest pattern *prefix* matching the current suffix (bytes to hold back). */
  def depthAt(state: Int): Int = depth(state)

  /** Length of the longest *complete* pattern ending at this state, or 0. */
  def matchLenAt(state: Int): Int = outLen(state)

  /** Id of the pattern reported by [[matchLenAt]], or -1. */
  def matchIdAt(state: Int): Int = outId(state)
}

object AhoCorasick {

  /** Build an automaton from patterns. Pattern `i` is reported with id `i`. */
  def apply(patterns: Seq[Array[Byte]]): AhoCorasick = {
    require(patterns.nonEmpty, "at least one pattern required")
    require(patterns.forall(_.nonEmpty), "patterns must be non-empty")

    // --- 1. Build the trie (sparse: -1 means "no edge") ---
    val next    = mutable.ArrayBuffer[Array[Int]](Array.fill(256)(-1))
    val depth   = mutable.ArrayBuffer[Int](0)
    val termLen = mutable.ArrayBuffer[Int](0)
    val termId  = mutable.ArrayBuffer[Int](-1)
    var maxLen  = 0

    patterns.zipWithIndex.foreach { case (pat, id) =>
      var s = 0
      for (b <- pat) {
        val ub = b & 0xff
        if (next(s)(ub) == -1) {
          next(s)(ub) = next.size
          next   += Array.fill(256)(-1)
          depth  += depth(s) + 1
          termLen += 0
          termId  += -1
        }
        s = next(s)(ub)
      }
      // Longest pattern ending exactly at this node wins (patterns may collide).
      if (pat.length > termLen(s)) { termLen(s) = pat.length; termId(s) = id }
      maxLen = math.max(maxLen, pat.length)
    }

    val n      = next.size
    val fail   = Array.fill(n)(0)
    val outLen = Array.fill(n)(0)
    val outId  = Array.fill(n)(-1)

    // --- 2. BFS to resolve fail links and fill the DFA transition table ---
    val queue = mutable.Queue[Int]()
    val root  = next(0)
    var c = 0
    while (c < 256) {
      if (root(c) == -1) root(c) = 0 // root self-loops on unknown bytes
      else {
        fail(root(c)) = 0
        setOutput(root(c), termLen, termId, outLen, outId, fail)
        queue.enqueue(root(c))
      }
      c += 1
    }

    while (queue.nonEmpty) {
      val u = queue.dequeue()
      var ch = 0
      while (ch < 256) {
        val v = next(u)(ch)
        if (v == -1) {
          next(u)(ch) = next(fail(u))(ch) // DFA: follow fail link, pre-resolved
        } else {
          fail(v) = next(fail(u))(ch)
          setOutput(v, termLen, termId, outLen, outId, fail)
          queue.enqueue(v)
        }
        ch += 1
      }
    }

    new AhoCorasick(next.toArray, depth.toArray, outLen, outId, maxLen)
  }

  /** A state's output is its own pattern if terminal, else the best of its fail target. */
  private def setOutput(
      v: Int,
      termLen: mutable.ArrayBuffer[Int],
      termId: mutable.ArrayBuffer[Int],
      outLen: Array[Int],
      outId: Array[Int],
      fail: Array[Int]
  ): Unit = {
    if (termLen(v) > 0) { outLen(v) = termLen(v); outId(v) = termId(v) }
    else { outLen(v) = outLen(fail(v)); outId(v) = outId(fail(v)) } // fail(v) shallower, already set
  }
}
