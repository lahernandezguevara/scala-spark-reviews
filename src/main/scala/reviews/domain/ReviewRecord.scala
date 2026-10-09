package reviews.domain

final case class ReviewRecord(
  id: String,
  sourceOrder: Long,
  title: String,
  review: String,
  town: Option[String],
  region: Option[String],
  placeType: Option[String],
  polarity: Option[Int]
)
