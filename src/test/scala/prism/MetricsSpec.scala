package prism

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class MetricsSpec extends AnyWordSpec with Matchers {

  "Metrics" should {
    "count requests by status and render Prometheus text" in {
      val m = new Metrics()
      m.record(200, 0.01)
      m.record(200, 0.2)
      m.record(502, 0.001)
      val out = m.render()

      out should include ("""prism_requests_total{status="200"} 2""")
      out should include ("""prism_requests_total{status="502"} 1""")
      out should include ("prism_upstream_errors_total 1")          // 502 counts as upstream error
      out should include ("prism_request_duration_seconds_count 3")
      // cumulative histogram: le="0.005" sees only the 0.001 observation
      out should include ("""prism_request_duration_seconds_bucket{le="0.005"} 1""")
      out should include ("""prism_request_duration_seconds_bucket{le="+Inf"} 3""")
    }

    "emit HELP/TYPE lines for each metric" in {
      val out = new Metrics().render()
      out should include ("# TYPE prism_requests_total counter")
      out should include ("# TYPE prism_upstream_errors_total counter")
      out should include ("# TYPE prism_request_duration_seconds histogram")
    }
  }
}
