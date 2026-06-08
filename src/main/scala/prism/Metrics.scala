package prism

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{DoubleAdder, LongAdder}
import scala.jdk.CollectionConverters.*

/**
 * Minimal Prometheus metrics for the proxy: request counts by status code, a latency
 * histogram, and an upstream-error counter. Hand-rolled exposition format so there is
 * no extra dependency. Thread-safe via `LongAdder`/`DoubleAdder`.
 */
final class Metrics {

  private val byStatus    = new ConcurrentHashMap[Int, LongAdder]()
  private val upstreamErr = new LongAdder()
  // Cumulative-by-construction histogram: an observation increments every bucket >= it.
  private val bounds      = Array(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
  private val bucketHits  = Array.fill(bounds.length)(new LongAdder())
  private val totalCount  = new LongAdder()
  private val totalSum    = new DoubleAdder()

  /** Record one request: its status code and its duration in seconds. */
  def record(status: Int, seconds: Double): Unit = {
    byStatus.computeIfAbsent(status, _ => new LongAdder()).increment()
    if (status == 502 || status == 504) upstreamErr.increment()
    var i = 0
    while (i < bounds.length) { if (seconds <= bounds(i)) bucketHits(i).increment(); i += 1 }
    totalCount.increment()
    totalSum.add(seconds)
  }

  /** Render the Prometheus text exposition format. */
  def render(): String = {
    val sb = new StringBuilder
    sb.append("# HELP prism_requests_total Total HTTP requests by status code.\n")
    sb.append("# TYPE prism_requests_total counter\n")
    byStatus.asScala.toSeq.sortBy(_._1).foreach { case (st, c) =>
      sb.append(s"""prism_requests_total{status="$st"} ${c.sum()}""").append('\n')
    }
    sb.append("# HELP prism_upstream_errors_total Upstream failures (502/504).\n")
    sb.append("# TYPE prism_upstream_errors_total counter\n")
    sb.append(s"prism_upstream_errors_total ${upstreamErr.sum()}\n")
    sb.append("# HELP prism_request_duration_seconds Request duration in seconds.\n")
    sb.append("# TYPE prism_request_duration_seconds histogram\n")
    var i = 0
    while (i < bounds.length) {
      sb.append(s"""prism_request_duration_seconds_bucket{le="${bounds(i)}"} ${bucketHits(i).sum()}""").append('\n')
      i += 1
    }
    val count = totalCount.sum()
    sb.append(s"""prism_request_duration_seconds_bucket{le="+Inf"} $count""").append('\n')
    sb.append(s"prism_request_duration_seconds_sum ${totalSum.sum()}\n")
    sb.append(s"prism_request_duration_seconds_count $count\n")
    sb.toString
  }
}
