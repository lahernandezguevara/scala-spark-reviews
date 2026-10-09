package reviews.domain

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.funsuite.AnyFunSuite

final class ReviewRecordSpec extends AnyFunSuite {
  test("orders unlabeled reviews by their source position") {
    val conf = new SparkConf()
      .setAppName("review-contract-test")
      .setMaster("local[2]")
      .set("spark.ui.enabled", "false")

    val sc = new SparkContext(conf)
    try {
      val later = ReviewRecord("r-2", 1L, "", "Good", None, None, None, None)
      val earlier = ReviewRecord("r-1", 0L, "", "Bad", None, None, None, None)

      val ordered = sc.parallelize(Seq(later, earlier), 2)
        .sortBy(_.sourceOrder)
        .map(r => (r.id, r.polarity))
        .collect()
        .toSeq

      assert(ordered == Seq(("r-1", None), ("r-2", None)))
    } finally {
      sc.stop()
    }
  }
}
