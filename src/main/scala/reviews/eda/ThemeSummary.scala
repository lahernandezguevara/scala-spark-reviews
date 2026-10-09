package reviews.eda

case class ThemeSummary(
  name: String,
  representativeTerms: Seq[String],
  distinctReviewsCount: Long,
  denominator: Long,
  evidenceSourceOrders: Seq[Long]
)