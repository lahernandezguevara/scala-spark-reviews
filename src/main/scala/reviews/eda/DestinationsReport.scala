package presentation

import org.apache.spark.rdd.RDD

/**
 * Capa de presentación: consume RDDs ya agregados por `analytics.Destinations`,
 * materializa resultados vía `collect()` y los renderiza como tablas de texto.
 * No contiene lógica de agregación (separación de preocupaciones).
 */
object DestinationsReport {

  private val Separator: String = "-" * 60

  def printWarnings(): Unit = {
    println(Separator)
    println("ALCANCE: El ranking mide volumen de reseñas en la muestra, no afluencia real de turistas.")
    println("SIGNIFICANCIA: Variaciones en destinos con tamaño de muestra bajo carecen de significancia estadística; no implican causalidad inherente al destino.")
    println("RESTRICCIÓN: La variable Town se limita a EDA para detectar sesgos. No se utilizará como feature predictivo por la incertidumbre de su disponibilidad en datos de evaluación a ciegas.")
    println(Separator)
  }

  def printFrequencyAndProportion(
      frequency: RDD[(String, Long)],
      proportion: RDD[(String, Double)]
  ): Unit = {
    // Se combinan por clave en el driver (post-collect) porque el volumen
    // de destinos únicos es bajo comparado con el de reseñas crudas.
    val freqMap = frequency.collect().toMap
    val propMap = proportion.collect().toMap

    println("TABLA: Frecuencia y proporción por destino")
    println(f"${"Town"}%-25s ${"Count"}%10s ${"Proportion"}%12s")
    freqMap.keySet.toSeq.sorted.foreach { town =>
      val count = freqMap(town)
      val prop = propMap.getOrElse(town, 0.0)
      println(f"$town%-25s $count%10d ${prop * 100}%11.2f%%")
    }
    println()
  }

  def printCrossByTownAndType(cross: RDD[((String, String), Long)]): Unit = {
    println("TABLA: Cruce Destino x Tipo de reseña")
    println(f"${"Town"}%-25s ${"ReviewType"}%-20s ${"Count"}%10s")
    cross.collect().sortBy { case ((town, reviewType), _) => (town, reviewType) }.foreach {
      case ((town, reviewType), count) =>
        println(f"$town%-25s $reviewType%-20s $count%10d")
    }
    println()
  }

  def printCrossByTownAndPolarity(cross: RDD[((String, Int), Long)]): Unit = {
    println("TABLA: Cruce Destino x Polaridad")
    println(f"${"Town"}%-25s ${"Polarity"}%10s ${"Count"}%10s")
    cross.collect().sortBy { case ((town, polarity), _) => (town, polarity) }.foreach {
      case ((town, polarity), count) =>
        println(f"$town%-25s $polarity%10d $count%10d")
    }
    println()
  }

  def printFullReport(
      frequency: RDD[(String, Long)],
      proportion: RDD[(String, Double)],
      crossByType: RDD[((String, String), Long)],
      crossByPolarity: RDD[((String, Int), Long)]
  ): Unit = {
    printWarnings()
    printFrequencyAndProportion(frequency, proportion)
    printCrossByTownAndType(crossByType)
    printCrossByTownAndPolarity(crossByPolarity)
  }
}
