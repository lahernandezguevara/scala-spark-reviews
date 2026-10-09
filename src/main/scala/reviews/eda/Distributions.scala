package reviews.eda

import org.apache.spark.rdd.RDD
import _root_.reviews.domain.ReviewRecord

case class ClassDistribution(className: String, count: Long, denominator: Long, percentage: Double)
case class InvalidDistribution(description: String, count: Long, total: Long, percentage: Double)
case class ImbalanceMetrics(majorityClass: String, minorityClass: String, ratio: Option[Double])

case class VariableReport(
  validStats: Seq[ClassDistribution],
  missing: InvalidDistribution,
  invalid: InvalidDistribution,
  imbalance: Option[ImbalanceMetrics]
)

case class DistributionsReport(
  totalRecords: Long,
  typeReport: VariableReport,
  polarityReport: VariableReport,
  explanation: String
) {
  def printReport(): Unit = {
    println("=" * 60)
    println("REPORTE DE DISTRIBUCIONES Y DESBALANCE")
    println("=" * 60)
    println(s"Total de registros evaluados: $totalRecords\n")

    def printVarReport(name: String, rep: VariableReport): Unit = {
      println(s"--- Distribución de $name ---")
      println(f"${"Clase"}%-15s ${"Conteo"}%10s ${"Denominador"}%15s ${"Porcentaje"}%12s")
      rep.validStats.foreach { s =>
        println(f"${s.className}%-15s ${s.count}%10d ${s.denominator}%15d ${s.percentage}%11.2f%%")
      }
      println("\nRegistros Ausentes e Inválidos:")
      println(f"  ${rep.missing.description}%-20s -> ${rep.missing.count}%7d (${rep.missing.percentage}%.2f%% del total)")
      println(f"  ${rep.invalid.description}%-20s -> ${rep.invalid.count}%7d (${rep.invalid.percentage}%.2f%% del total)\n")
      
      rep.imbalance.foreach { i =>
        println(s"Clase Mayoritaria: ${i.majorityClass}")
        println(s"Clase Minoritaria: ${i.minorityClass}")
        val ratioStr = i.ratio.map(r => f"$r%.2f").getOrElse("No definida (conteo minoritario es cero)")
        println(s"Razón de Desbalance (Mayoritaria/Minoritaria): $ratioStr\n")
      }
    }

    printVarReport("Type", typeReport)
    printVarReport("Polarity", polarityReport)
    
    println("--- Análisis de Métricas ---")
    println(explanation)
    println("=" * 60)
  }
}

object Distributions {
  private val validTypes = Seq("Restaurant", "Attractive", "Hotel")
  private val validPolarities = Seq(1, 2, 3, 4, 5)

  private def calcPercentage(count: Long, denominator: Long): Double = 
    if (denominator == 0) 0.0 else (count.toDouble / denominator.toDouble) * 100.0

  def analyze(rdd: RDD[ReviewRecord]): DistributionsReport = {
    // Se elimina el manejo interno de caché (rdd.cache/unpersist)
    // para respetar la gestión de persistencia del llamador.
    val total = rdd.count()

    val typeCounts = rdd.map { r =>
      r.placeType match {
        case None => ("Missing", 1L)
        case Some(t) if validTypes.contains(t) => (t, 1L)
        case Some(_) => ("Invalid", 1L)
      }
    }.reduceByKey(_ + _).collect().toMap

    val polarityCounts = rdd.map { r =>
      r.polarity match {
        case None => ("Missing", 1L)
        case Some(p) if validPolarities.contains(p) => (p.toString, 1L)
        case Some(_) => ("Invalid", 1L)
      }
    }.reduceByKey(_ + _).collect().toMap

    // 2. Procesar estadísticas de Type
    val typeMissing = typeCounts.getOrElse("Missing", 0L)
    val typeInvalid = typeCounts.getOrElse("Invalid", 0L)
    // Se asegura la inclusión de todas las clases válidas esperadas (conteo 0 si no aparecen)
    val typeValidCounts = validTypes.map(t => (t, typeCounts.getOrElse(t, 0L)))
    val typeValidDenominator = typeValidCounts.map(_._2).sum

    val typeStats = typeValidCounts.map { case (t, count) =>
      ClassDistribution(t, count, typeValidDenominator, calcPercentage(count, typeValidDenominator))
    }

    val typeImbalance = if (typeValidDenominator > 0) {
      val sorted = typeValidCounts.sortBy(_._2)
      val minority = sorted.head
      val majority = sorted.last
      val ratio = if (minority._2 == 0) None else Some(majority._2.toDouble / minority._2.toDouble)
      Some(ImbalanceMetrics(majority._1, minority._1, ratio))
    } else None

    val typeReport = VariableReport(
      validStats = typeStats,
      missing = InvalidDistribution("Type ausente", typeMissing, total, calcPercentage(typeMissing, total)),
      invalid = InvalidDistribution("Type inválido", typeInvalid, total, calcPercentage(typeInvalid, total)),
      imbalance = typeImbalance
    )

    // 3. Procesar estadísticas de Polarity
    val polMissing = polarityCounts.getOrElse("Missing", 0L)
    val polInvalid = polarityCounts.getOrElse("Invalid", 0L)
    val polValidCounts = validPolarities.map(p => (p.toString, polarityCounts.getOrElse(p.toString, 0L)))
    val polValidDenominator = polValidCounts.map(_._2).sum

    val polStats = polValidCounts.map { case (p, count) =>
      ClassDistribution(p, count, polValidDenominator, calcPercentage(count, polValidDenominator))
    }

    val polImbalance = if (polValidDenominator > 0) {
      val sorted = polValidCounts.sortBy(_._2)
      val minority = sorted.head
      val majority = sorted.last
      val ratio = if (minority._2 == 0) None else Some(majority._2.toDouble / minority._2.toDouble)
      Some(ImbalanceMetrics(majority._1, minority._1, ratio))
    } else None

    val polarityReport = VariableReport(
      validStats = polStats,
      missing = InvalidDistribution("Polarity ausente", polMissing, total, calcPercentage(polMissing, total)),
      invalid = InvalidDistribution("Polarity inválida", polInvalid, total, calcPercentage(polInvalid, total)),
      imbalance = polImbalance
    )

    val explanation = 
      """En conjuntos de datos con clases fuertemente desbalanceadas, la Exactitud (Accuracy) puede resultar engañosa.
        |Un modelo podría limitarse a predecir siempre la clase mayoritaria y obtener una exactitud muy alta, 
        |ocultando su incapacidad para identificar correctamente las clases minoritarias.
        |El Macro F1 es relevante porque calcula la métrica F1 de forma independiente para cada clase y promedia los resultados 
        |sin ponderar por el número de instancias, penalizando fuertemente a los modelos que ignoran a la minoría.""".stripMargin

    DistributionsReport(total, typeReport, polarityReport, explanation)
  }
}