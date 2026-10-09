// src/main/scala/reviews/ml/TextFeatures.scala
package reviews.ml

import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

final case class FeaturedReview(
  id: String,
  sourceOrder: Long,
  typeLabel: Option[Int],
  polarityLabel: Option[Int],
  features: Vector
)

final case class FeatureSplitReport(
  inputCount: Long,
  outputCount: Long,
  emptyTokenCount: Long,
  validTypeLabelCount: Long,
  validPolarityLabelCount: Long
)

final case class FeatureReport(
  numFeatures: Int,
  idfFitDocumentCount: Long,
  train: FeatureSplitReport,
  validation: FeatureSplitReport,
  holdout: FeatureSplitReport
)

final case class FeatureSplits(
  train: RDD[FeaturedReview],
  validation: RDD[FeaturedReview],
  holdout: RDD[FeaturedReview],
  report: FeatureReport
)

object TextFeatures {

  val NumFeatures: Int = 65536

  // Agregado por partición: (filas, sin tokens, type válido, polarity válido)
  private type Agg = Array[Long]

  def fitAndTransform(splits: DatasetSplits): FeatureSplits = {
    val hashingTF = new HashingTF(NumFeatures)

    // (reseña con vector de conteos, indicador de texto sin tokens)
    def toTermFrequencies(rdd: RDD[EncodedReview]): RDD[(FeaturedReview, Boolean)] =
      rdd.map { r =>
        val tokens = TextPreprocessor.tokenize(r.modelText)
        val tf = hashingTF.transform(tokens) // sin tokens => vector disperso vacío de tamaño NumFeatures
        (FeaturedReview(r.id, r.sourceOrder, r.typeLabel, r.polarityLabel, tf), tokens.isEmpty)
      }

    val trainTf = toTermFrequencies(splits.train).persist(StorageLevel.MEMORY_AND_DISK)
    val validationTf = toTermFrequencies(splits.validation).persist(StorageLevel.MEMORY_AND_DISK)
    val holdoutTf = toTermFrequencies(splits.holdout).persist(StorageLevel.MEMORY_AND_DISK)

    try {
      // IDF ajustado exclusivamente con los vectores de train
      // (incluye los vectores cero de reseñas sin tokens).
      val idfModel = new IDF().fit(trainTf.map(_._1.features))

      def applyIdf(rdd: RDD[(FeaturedReview, Boolean)]): RDD[FeaturedReview] =
        rdd.map { case (review, _) =>
          review.copy(features = idfModel.transform(review.features))
        }

      val trainOut = applyIdf(trainTf)
      val validationOut = applyIdf(validationTf)
      val holdoutOut = applyIdf(holdoutTf)

      val trainAgg = aggregate(trainTf)
      val trainReport = buildReport(splits.train, trainOut, trainAgg)
      val validationReport = buildReport(splits.validation, validationOut, aggregate(validationTf))
      val holdoutReport = buildReport(splits.holdout, holdoutOut, aggregate(holdoutTf))

      val report = FeatureReport(
        numFeatures = NumFeatures,
        idfFitDocumentCount = trainAgg(0),
        train = trainReport,
        validation = validationReport,
        holdout = holdoutReport
      )

      FeatureSplits(trainOut, validationOut, holdoutOut, report)
    } finally {
      trainTf.unpersist(blocking = false)
      validationTf.unpersist(blocking = false)
      holdoutTf.unpersist(blocking = false)
    }
  }

  private def aggregate(rows: RDD[(FeaturedReview, Boolean)]): Agg = {
    val zero: Agg = Array(0L, 0L, 0L, 0L)
    rows.aggregate(zero)(
      (acc, row) => {
        val (r, empty) = row
        Array(
          acc(0) + 1L,
          acc(1) + (if (empty) 1L else 0L),
          acc(2) + (if (r.typeLabel.exists(_ >= 0)) 1L else 0L),
          acc(3) + (if (r.polarityLabel.exists(_ >= 0)) 1L else 0L)
        )
      },
      (a, b) => Array(a(0) + b(0), a(1) + b(1), a(2) + b(2), a(3) + b(3))
    )
  }

  private def buildReport(
    input: RDD[EncodedReview],
    output: RDD[FeaturedReview],
    agg: Agg
  ): FeatureSplitReport =
    FeatureSplitReport(
      inputCount = input.count(),
      outputCount = output.count(),
      emptyTokenCount = agg(1),
      validTypeLabelCount = agg(2),
      validPolarityLabelCount = agg(3)
    )
}