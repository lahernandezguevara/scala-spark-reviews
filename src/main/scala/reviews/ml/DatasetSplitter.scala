package reviews.ml

import org.apache.spark.rdd.RDD
import reviews.domain.ReviewRecord
import java.security.MessageDigest
import java.math.{BigInteger, BigDecimal, RoundingMode}
import java.util.Locale

final case class EncodedReview(
  id: String,
  sourceOrder: Long,
  modelText: String,
  typeLabel: Option[Int],
  polarityLabel: Option[Int]
)

final case class TargetLabelCounts(
  missing: Long,
  invalid: Long,
  byClass: Map[Int, Long]
)

final case class SplitCounts(
  records: Long,
  typeLabels: TargetLabelCounts,
  polarityLabels: TargetLabelCounts
)

final case class SplitReport(
  totalRecords: Long,
  partitions: Map[String, SplitCounts],
  duplicateTextGroups: Long,
  duplicateTypeLabelConflictGroups: Long,
  duplicatePolarityLabelConflictGroups: Long,
  duplicateGroupsAcrossPartitions: Long,
  seed: Long,
  method: String
)

final case class DatasetSplits(
  train: RDD[EncodedReview],
  validation: RDD[EncodedReview],
  holdout: RDD[EncodedReview],
  report: SplitReport
)

object DatasetSplitter {
  
  def split(records: RDD[ReviewRecord], seed: Long = 42L): DatasetSplits = {
    // 1. Preprocesamiento: Construir modelText, llave canónica y codificar etiquetas
    val encodedWithKey = records.map { record =>
      // Codificación Type
      val tLabel = record.placeType.map {
        case "Attractive" => 0
        case "Hotel"      => 1
        case "Restaurant" => 2
        case _            => -1
      }
      
      // Codificación Polarity
      val pLabel = record.polarity.map {
        case v if v >= 1 && v <= 5 => v - 1
        case _                     => -1 
      }
      
      val modelText = s"${record.title} ${record.review}"
      
      // Llave canónica: quitar extremos, colapsar espacios y minúsculas seguras
      val canonicalKey = modelText
        .trim
        .replaceAll("\\s+", " ")
        .toLowerCase(Locale.ROOT)
      
      val encoded = EncodedReview(
        id = record.id,
        sourceOrder = record.sourceOrder,
        modelText = modelText,
        typeLabel = tLabel,
        polarityLabel = pLabel
      )
      
      (canonicalKey, encoded)
    }

    // Asignación determinista de partición
    val partitioned = encodedWithKey.map { case (canonicalKey, encoded) =>
      val fraction = getPartitionFraction(canonicalKey, seed)
      val partitionName = fraction match {
        case f if f >= 0.00 && f < 0.70 => "train"
        case f if f >= 0.70 && f < 0.90 => "validation"
        case _                          => "holdout"
      }
      (canonicalKey, partitionName, encoded)
    }
    
    partitioned.cache()

    // RDDs finales
    val train = partitioned.filter(_._2 == "train").map(_._3).cache()
    val validation = partitioned.filter(_._2 == "validation").map(_._3).cache()
    val holdout = partitioned.filter(_._2 == "holdout").map(_._3).cache()

    // auditoría de duplicados mediante Map-Reduce (sin extraer al driver)
    val totalRecords = records.count()
    
    // Agregamos por llave canónica: (ParticionesVistas, TypesVistos, PolaritiesVistas, Conteo)
    val aggByKey = partitioned.map { case (key, part, enc) =>
      (key, (Set(part), Set(enc.typeLabel), Set(enc.polarityLabel), 1L))
    }.reduceByKey { case ((part1, t1, p1, c1), (part2, t2, p2, c2)) =>
      (part1 ++ part2, t1 ++ t2, p1 ++ p2, c1 + c2)
    }
    
    val duplicateGroups = aggByKey.filter(_._2._4 > 1).cache()
    
    val report = SplitReport(
      totalRecords = totalRecords,
      partitions = Map(
        "train" -> calculateCounts(train),
        "validation" -> calculateCounts(validation),
        "holdout" -> calculateCounts(holdout)
      ),
      duplicateTextGroups = duplicateGroups.count(),
      duplicateTypeLabelConflictGroups = duplicateGroups.filter(_._2._2.size > 1).count(),
      duplicatePolarityLabelConflictGroups = duplicateGroups.filter(_._2._3.size > 1).count(),
      duplicateGroupsAcrossPartitions = duplicateGroups.filter(_._2._1.size > 1).count(),
      seed = seed,
      method = "SHA-256(seed+canonicalKey) [primeros 8 bytes sin signo] mod 2^64 => [0,1)"
    )

    duplicateGroups.unpersist()
    partitioned.unpersist()

    DatasetSplits(train, validation, holdout, report)
  }

  private def getPartitionFraction(canonicalKey: String, seed: Long): Double = {
    val md = MessageDigest.getInstance("SHA-256")
    val input = s"$seed$canonicalKey".getBytes("UTF-8")
    val first8 = md.digest(input).take(8)
    
    // new BigInteger(1, bytes) fuerza la interpretación como entero sin signo positivo
    val unsignedInt = new BigDecimal(new BigInteger(1, first8))
    val twoTo64 = new BigDecimal("18446744073709551616") 
    unsignedInt.divide(twoTo64, 15, RoundingMode.DOWN).doubleValue()
  }

  private def calculateCounts(rdd: RDD[EncodedReview]): SplitCounts = {
    val total = rdd.count()
    
    val stats = rdd.map { r =>
      Array(
        if (r.typeLabel.isEmpty) 1L else 0L,
        if (r.typeLabel.exists(_ < 0)) 1L else 0L,
        if (r.typeLabel.contains(0)) 1L else 0L,
        if (r.typeLabel.contains(1)) 1L else 0L,
        if (r.typeLabel.contains(2)) 1L else 0L,
        if (r.polarityLabel.isEmpty) 1L else 0L,
        if (r.polarityLabel.exists(_ < 0)) 1L else 0L,
        if (r.polarityLabel.contains(0)) 1L else 0L,
        if (r.polarityLabel.contains(1)) 1L else 0L,
        if (r.polarityLabel.contains(2)) 1L else 0L,
        if (r.polarityLabel.contains(3)) 1L else 0L,
        if (r.polarityLabel.contains(4)) 1L else 0L
      )
    }.reduce { (a, b) => a.zip(b).map(x => x._1 + x._2) }
    
    val typeCounts = TargetLabelCounts(
      missing = stats(0), invalid = stats(1),
      byClass = Map(0 -> stats(2), 1 -> stats(3), 2 -> stats(4))
    )
    
    val polarityCounts = TargetLabelCounts(
      missing = stats(5), invalid = stats(6),
      byClass = Map(0 -> stats(7), 1 -> stats(8), 2 -> stats(9), 3 -> stats(10), 4 -> stats(11))
    )

    SplitCounts(total, typeCounts, polarityCounts)
  }
}