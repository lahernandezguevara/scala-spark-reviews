package reviews.ml

final case class ClassificationPrediction(
  id: String,
  sourceOrder: Long,
  target: String,
  experiment: String,
  split: String,
  trueLabel: Int,
  predictedLabel: Int
) {
  require(id != null && id.trim.nonEmpty, "Prediction id must not be empty.")
  require(sourceOrder >= 0L, "sourceOrder must be non-negative.")

  require(
    target == ClassificationPrediction.Target.Type ||
      target == ClassificationPrediction.Target.Polarity,
    s"Unsupported target: $target"
  )

  require(
    experiment == ClassificationPrediction.Experiment.Baseline ||
      experiment == ClassificationPrediction.Experiment.Undersampled,
    s"Unsupported experiment: $experiment"
  )

  require(
    split == ClassificationPrediction.Split.Validation ||
      split == ClassificationPrediction.Split.Holdout,
    s"Unsupported split: $split"
  )

  private val maxLabel =
    if (target == ClassificationPrediction.Target.Type) 2 else 4

  require(
    trueLabel >= 0 && trueLabel <= maxLabel,
    s"trueLabel $trueLabel is invalid for target $target"
  )
  require(
    predictedLabel >= 0 && predictedLabel <= maxLabel,
    s"predictedLabel $predictedLabel is invalid for target $target"
  )
  require(
    experiment != ClassificationPrediction.Experiment.Undersampled ||
      target == ClassificationPrediction.Target.Polarity,
    "The undersampled experiment is only defined for Polarity."
  )
}

object ClassificationPrediction {
  object Target {
    val Type: String = "Type"
    val Polarity: String = "Polarity"
  }

  object Experiment {
    val Baseline: String = "baseline"
    val Undersampled: String = "undersampled"
  }

  object Split {
    val Validation: String = "validation"
    val Holdout: String = "holdout"
  }
}
