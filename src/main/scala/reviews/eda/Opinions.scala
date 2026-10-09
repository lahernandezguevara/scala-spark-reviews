package reviews.eda

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import _root_.reviews.domain.ReviewRecord

import java.util.Locale

case class OpinionStats(
  topComplaints: Seq[(String, Int)],
  topStrengths: Seq[(String, Int)],
  complaintsCount: Long,
  strengthsCount: Long,
  neutralCount: Long,
  invalidOrMissingCount: Long
)

object Opinions {

  private val negations = Set("no", "nunca", "sin")

  private val stopWords =
    Set(
      "el", "la", "los", "las", "un", "una", "unos", "unas",
      "de", "del", "a", "al", "en", "por", "para", "con",
      "su", "sus", "y", "o", "pero", "que", "es", "son",
      "fue", "muy", "mas", "más", "lo", "se", "me", "nos",
      "este", "era", "si", "mi", "todo", "como", "esta",
      "está", "ya", "hay", "tiene", "hasta", "desde"
    ) -- negations

  private val topOrdering: Ordering[(String, Int)] =
    Ordering.by[(String, Int), (Int, String)] {
      case (term, count) => (-count, term)
    }

  def analyze(reviews: RDD[ReviewRecord]): OpinionStats = {

    val categorized = reviews
      .map { r =>
        val category = r.polarity match {
          case Some(1 | 2) => "complaint"
          case Some(4 | 5) => "strength"
          case Some(3)    => "neutral"
          case _          => "invalid"
        }
        (category, r.review)
      }
      .persist(StorageLevel.MEMORY_AND_DISK)

    try {
      val categoryCounts = categorized.map(_._1).countByValue()

      val complaints = categorized.filter(_._1 == "complaint").map(_._2)
      val strengths = categorized.filter(_._1 == "strength").map(_._2)

      OpinionStats(
        topComplaints = extractNgrams(complaints, 10),
        topStrengths = extractNgrams(strengths, 10),
        complaintsCount = categoryCounts.getOrElse("complaint", 0L),
        strengthsCount = categoryCounts.getOrElse("strength", 0L),
        neutralCount = categoryCounts.getOrElse("neutral", 0L),
        invalidOrMissingCount = categoryCounts.getOrElse("invalid", 0L)
      )
    } finally {
      categorized.unpersist(blocking = false)
    }
  }

  private def extractNgrams(textRDD: RDD[String], topN: Int): Seq[(String, Int)] = {
    textRDD
      .flatMap { text =>
        val normalized = text.toLowerCase(Locale.ROOT).replaceAll("[^\\p{L}]", " ")

        val words = normalized.split("\\s+").iterator
            .filter(_.nonEmpty)
            .filter { w => w.length > 1 && (!stopWords.contains(w) || negations.contains(w)) }
            .toVector

        val unigrams = words.iterator
        val bigrams = words.sliding(2).map(_.mkString(" "))

        (unigrams ++ bigrams).toSet.iterator
      }
      .map(token => (token, 1))
      .reduceByKey(_ + _)
      .takeOrdered(topN)(topOrdering)
      .toSeq
  }
}