package prism

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class AhoCorasickSpec extends AnyWordSpec with Matchers {

  /** Drive the automaton over `text`, collecting (endIndexInclusive, matchedPatternId). */
  private def scan(ac: AhoCorasick, text: String): List[(Int, Int)] = {
    var state = ac.root
    val acc   = List.newBuilder[(Int, Int)]
    val bytes = text.getBytes("UTF-8")
    var i = 0
    while (i < bytes.length) {
      state = ac.step(state, bytes(i))
      if (ac.matchLenAt(state) > 0) acc += ((i, ac.matchIdAt(state)))
      i += 1
    }
    acc.result()
  }

  private def pat(ss: String*): Seq[Array[Byte]] = ss.map(_.getBytes("UTF-8"))

  "AhoCorasick" should {
    "find classic overlapping dictionary matches" in {
      // patterns: he(0) she(1) his(2) hers(3) — the textbook example
      val ac = AhoCorasick(pat("he", "she", "his", "hers"))
      val hits = scan(ac, "ushers")
      // "she" ends at idx 3, "he" ends at idx 3, "hers" ends at idx 5.
      // matchLenAt reports the LONGEST ending at each index:
      hits should contain((3, 1)) // "she"
      hits should contain((5, 3)) // "hers"
    }

    "report the longest pattern when several end at the same index" in {
      val ac = AhoCorasick(pat("cd", "abcd"))
      val hits = scan(ac, "abcd")
      hits should contain((3, 1)) // "abcd", not "cd"
      hits.filter(_._1 == 3).map(_._2) shouldBe List(1)
    }

    "expose prefix depth (bytes to hold back) correctly" in {
      val ac = AhoCorasick(pat("abcd"))
      var s = ac.root
      val bs = "abxabc".getBytes("UTF-8")
      val depths = bs.map { b => s = ac.step(s, b); ac.depthAt(s) }.toList
      // a=1, ab=2, x breaks ->0, a=1, ab=2, abc=3
      depths shouldBe List(1, 2, 0, 1, 2, 3)
    }

    "self-loop at root on unknown bytes" in {
      val ac = AhoCorasick(pat("xyz"))
      scan(ac, "aaaa") shouldBe Nil
    }
  }
}
