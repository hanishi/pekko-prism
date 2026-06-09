package prism

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import zio.{Chunk, Runtime, Unsafe}
import zio.stream.ZStream

/**
 * The core engine, driven from ZIO Streams. Proves the rewriters port: the same
 * chunk-boundary correctness (and the same multi-pattern matchers) work through a
 * `ZPipeline`, with no Pekko Streams and no HTTP involved.
 */
class ZioRewriteSpec extends AnyWordSpec with Matchers {

  private def run(chunks: Seq[String], rw: Rewriter): String = {
    val byteChunks = chunks.map(s => Chunk.fromArray(s.getBytes("UTF-8")))
    val collected = Unsafe.unsafe { implicit u =>
      Runtime.default.unsafe
        .run(ZStream.fromChunks(byteChunks*).via(ZioRewrite.pipeline(rw)).runCollect)
        .getOrThrow()
    }
    new String(collected.toArray, "UTF-8")
  }

  "ZioRewrite.pipeline" should {
    "rewrite within a single chunk" in {
      run(Seq("a internal.example.com b"), BmhRewriter("internal.example.com", "X")) shouldBe "a X b"
    }

    "catch a match that straddles a chunk boundary (the whole point)" in {
      run(Seq("...internal.examp", "le.com..."), BmhRewriter("internal.example.com", "X")) shouldBe "...X..."
    }

    "drive a multi-pattern Wu-Manber rewrite, identically at every split" in {
      val rw   = new WuManberRewriter(Seq("internal.example.com" -> "X", "</head>" -> "Y"))
      val full = "a internal.example.com b </head> c internal.exa"
      val expected = run(Seq(full), rw)
      (1 to full.length).foreach(n => run(full.grouped(n).toSeq, rw) shouldBe expected)
    }
  }
}
