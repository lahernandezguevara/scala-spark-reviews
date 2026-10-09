package analytics

import org.apache.spark.rdd.RDD
import reviews.domain.ReviewRecord

/**
 * Agregaciones puras sobre RDD[ReviewRecord]. No realiza I/O ni gestiona
 * el ciclo de vida de SparkContext/SparkSession: opera exclusivamente
 * sobre RDDs inyectados para permitir pruebas unitarias aisladas.
 */
object Destinations {

  /**
   * Frecuencia absoluta por destino.
   */
  def frequencyByTown(records: RDD[ReviewRecord]): RDD[(String, Long)] =
    records
      .flatMap(record => record.town.map(_ -> 1L))
      .reduceByKey(_ + _)

  /**
   * Proporción de cada destino respecto al total de la muestra.
   *
   * Se recibe `total` como parámetro (en vez de recalcularlo con .count())
   * para evitar una acción adicional sobre el RDD cuando el llamador ya
   * conoce el tamaño de la muestra, y para permitir probar el cálculo
   * de proporción de forma determinista en tests.
   */
  def proportionByTown(records: RDD[ReviewRecord], total: Long): RDD[(String, Double)] = {
    require(total > 0, "El total de la muestra debe ser mayor a cero para calcular proporciones.")
    frequencyByTown(records).mapValues(count => count.toDouble / total.toDouble)
  }

  /**
   * Cruce de frecuencias destino x tipo de reseña.
   */
  def crossByTownAndType(records: RDD[ReviewRecord]): RDD[((String, String), Long)] =
    records
      .flatMap { record =>
        for {
          town <- record.town
          placeType <- record.placeType
        } yield ((town, placeType), 1L)
      }
      .reduceByKey(_ + _)

  /**
   * Cruce de frecuencias destino x polaridad.
   */
  def crossByTownAndPolarity(records: RDD[ReviewRecord]): RDD[((String, Int), Long)] =
    records
      .flatMap { record =>
        for {
          town <- record.town
          polarity <- record.polarity
        } yield ((town, polarity), 1L)
      }
      .reduceByKey(_ + _)
}
