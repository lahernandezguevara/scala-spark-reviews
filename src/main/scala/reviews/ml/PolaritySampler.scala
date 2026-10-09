package reviews.ml

import java.security.MessageDigest
import org.apache.spark.rdd.RDD

final case class PolaritySamplingReport(
  seed: Long,
  maxClassCount: Long,
  missingLabelCount: Long,
  invalidLabelCount: Long,
  classCountsBefore: Map[Int, Long],
  classCountsAfter: Map[Int, Long],
  retainedFractionByClass: Map[Int, Double],
  totalBefore: Long,
  totalAfter: Long
)

final case class PolarityTrainingSample(
  train: RDD[FeaturedReview],
  report: PolaritySamplingReport
)

object PolaritySampler {

    // Generador de prioridad estable usando SHA-256
  private def calculatePriority(seed: Long, id: String): String = {
    val input = s"${seed}${id}"
    val digest = MessageDigest.getInstance("SHA-256")
    val hashBytes = digest.digest(input.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }
  def undersamplePolarity(
    train: RDD[FeaturedReview],
    seed: Long = 42L
  ): PolarityTrainingSample = {
    
    // Aquí implementaremos toda la lógica de filtrado y conteo en los siguientes pasos

    val missingCount = train.filter(_.polarityLabel.isEmpty).count()
    val invalidCount = train.filter(r => 
      r.polarityLabel.isDefined && (r.polarityLabel.get < 0 || r.polarityLabel.get > 4)
    ).count()


    val validTrain = train.filter(r => 
      r.polarityLabel.isDefined && r.polarityLabel.get >= 0 && r.polarityLabel.get <= 4
    )


    val rawCounts = validTrain
      .map(r => (r.polarityLabel.get, 1L))
      .reduceByKey(_ + _)
      .collectAsMap()
      .toMap
    

    val classCountsBefore = (0 to 4).map(c => c -> rawCounts.getOrElse(c, 0L)).toMap

    val minClassCount = classCountsBefore.values.min
    val maxClassCount = minClassCount * 10
    
    val underLimitClasses = classCountsBefore.filter(_._2 <= maxClassCount).keys.toSet
    val overLimitClasses = classCountsBefore.filter(_._2 > maxClassCount).keys.toSet

    val idsToKeepArray = validTrain
      .filter(r => overLimitClasses.contains(r.polarityLabel.get))
      .map(r => (r.polarityLabel.get, (r.id, r.sourceOrder)))
      .groupByKey()
      .flatMap { case (_, idList) =>
        idList.toList
          .sortBy { case (id, order) => (calculatePriority(seed, id), order) }
          .take(maxClassCount.toInt)
          .map { case (id, _) => id }
      }
      .collect() // Traemos solo la lista de IDs ganadores al Driver

    val idsToKeepBroadcast = train.sparkContext.broadcast(idsToKeepArray.toSet)
    val underLimitBroadcast = train.sparkContext.broadcast(underLimitClasses)

    val sampledTrain = validTrain.filter { r =>
      val label = r.polarityLabel.get
      underLimitBroadcast.value.contains(label) || idsToKeepBroadcast.value.contains(r.id)
    }
    val classCountsAfterRaw = sampledTrain
      .map(r => (r.polarityLabel.get, 1L))
      .reduceByKey(_ + _)
      .collectAsMap()
      .toMap
      
    val classCountsAfter = (0 to 4).map(c => c -> classCountsAfterRaw.getOrElse(c, 0L)).toMap
    val retainedFraction = classCountsBefore.map { case (label, beforeCount) =>
      val afterCount = classCountsAfter(label)
      label -> (if (beforeCount == 0) 0.0 else afterCount.toDouble / beforeCount.toDouble)
    }


    val report = PolaritySamplingReport(
      seed = seed,
      maxClassCount = maxClassCount,
      missingLabelCount = missingCount,
      invalidLabelCount = invalidCount,
      classCountsBefore = classCountsBefore,
      classCountsAfter = classCountsAfter,
      retainedFractionByClass = retainedFraction,
      totalBefore = classCountsBefore.values.sum,
      totalAfter = classCountsAfter.values.sum
    )

    PolarityTrainingSample(sampledTrain, report)
    
  }
}