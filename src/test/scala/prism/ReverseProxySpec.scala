package prism

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.duration.*

class ReverseProxySpec extends AnyWordSpec with Matchers {

  import ReverseProxy.parse

  "ReverseProxy.parse" should {
    "default the positionals" in {
      val c = parse(Array("--rewrite", "a=b"))
      c.bindPort   shouldBe 8080
      c.originBase shouldBe "http://localhost:9001"
    }

    "read the port and origin positionals" in {
      val c = parse(Array("8443", "http://up:9000", "--rewrite", "a=b"))
      c.bindPort   shouldBe 8443
      c.originBase shouldBe "http://up:9000"
    }

    "map each flag to the right rule type, in order" in {
      val c = parse(Array(
        "--rewrite",       "a=b",
        "--rewrite-word",  "c=d",
        "--wrap-url",      "https://t=https://fp?{enc}",
        "--insert-before", "</head>=X",
        "--insert-after",  "<body>=Y"
      ))
      c.rules shouldBe List(
        Rule.Rewrite("a", "b"),
        Rule.RewriteWord("c", "d"),
        Rule.WrapUrl("https://t", "https://fp?{enc}"),
        Rule.InsertBefore("</head>", "X"),
        Rule.InsertAfter("<body>", "Y")
      )
    }

    "turn --public-host into a host rewrite using the origin authority" in {
      val c = parse(Array("8080", "http://internal:9001", "--public-host", "www.example.com"))
      c.rules shouldBe List(Rule.Rewrite("internal:9001", "www.example.com"))
    }

    "split anchor=value only on the first '='" in {
      parse(Array("--rewrite", "a=b=c")).rules shouldBe List(Rule.Rewrite("a", "b=c"))
    }

    "collect the boolean and pool flags" in {
      val c = parse(Array(
        "--rewrite", "a=b", "--text", "--attr", "--xml",
        "--tls", "ks.p12", "pw",
        "--pool-max-connections", "128", "--pool-max-open-requests", "512",
        "--pool-min-connections", "4", "--pool-idle-timeout", "45s", "--pool-pipelining-limit", "2"
      ))
      c.textOnly    shouldBe true
      c.attrScoped  shouldBe true
      c.acceptXml   shouldBe true
      c.tls         shouldBe Some(("ks.p12", "pw"))
      c.maxConns    shouldBe Some(128)
      c.maxOpenReqs shouldBe Some(512)
      c.minConns    shouldBe Some(4)
      c.idleTimeout shouldBe Some(45.seconds)
      c.pipelining  shouldBe Some(2)
    }

    "reject an unknown option" in {
      an[Exception] should be thrownBy parse(Array("--bogus"))
    }

    "require at least one rule" in {
      an[Exception] should be thrownBy parse(Array("8080", "http://u"))
    }

    "error when --tls is missing its password" in {
      an[Exception] should be thrownBy parse(Array("--rewrite", "a=b", "--tls", "ks.p12"))
    }
  }
}
