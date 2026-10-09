package reviews.eda

import org.apache.spark.rdd.RDD
import reviews.domain.ReviewRecord

case class QualityReport(
  totalRecords: Long,
  missingTitle: Long,
  missingReview: Long,
  missingPolarity: Long,
  invalidPolarity: Long,
  missingTown: Long,
  missingRegion: Long,
  missingPlaceType: Long,
  polarityDistribution: Map[String, Long], // para distribuciones
  typeDistribution: Map[String, Long]      //para distribuciones
)

object Quality {

  def analyze(rdd: RDD[ReviewRecord]): QualityReport = {
    // en memoria temporalmente porque haremo (count, reduce, countByValue)
    rdd.cache() 

    val totalRecords = rdd.count()

    val counts = rdd.map { record =>
      Array(
        if (record.title.trim.isEmpty) 1L else 0L,
        if (record.review.trim.isEmpty) 1L else 0L,
        
        if (record.polarity.isEmpty) 1L else 0L, // Ausente / Faltante
        if (record.polarity.exists(p => p < 1 || p > 5)) 1L else 0L,
        
        if (record.town.isEmpty || record.town.get.trim.isEmpty) 1L else 0L,
        if (record.region.isEmpty || record.region.get.trim.isEmpty) 1L else 0L,
        if (record.placeType.isEmpty || record.placeType.get.trim.isEmpty) 1L else 0L
      )
    }.reduce { (a, b) =>
      a.zip(b).map { case (x, y) => x + y }
    }

    
    val polarityDist = rdd.map(_.polarity.map(_.toString).getOrElse("Ausente")).countByValue().toMap
    val typeDist = rdd.map(_.placeType.getOrElse("Ausente")).countByValue().toMap

    val report = QualityReport(
      totalRecords = totalRecords,
      missingTitle = counts(0),
      missingReview = counts(1),
      missingPolarity = counts(2),
      invalidPolarity = counts(3),
      missingTown = counts(4),
      missingRegion = counts(5),
      missingPlaceType = counts(6),
      polarityDistribution = polarityDist,
      typeDistribution = typeDist
    )

    imprimirReporte(report)
    
    rdd.unpersist() //liber memoria
    report
  }

  private def imprimirReporte(report: QualityReport): Unit = {
    println("=" * 60)
    println("REPORTE DE CALIDAD DE DATOS - REST-MEX 2025")
    println("=" * 60)
    println(s"1. Número de registros procesados: ${report.totalRecords}")
    
    println("\n2. Valores ausentes o vacíos por campo:")
    println(s"   - Title: ${report.missingTitle}")
    println(s"   - Review: ${report.missingReview}")
    println(s"   - Polarity (Totalmente ausente): ${report.missingPolarity}")
    println(s"   - Town: ${report.missingTown}")
    println(s"   - Region: ${report.missingRegion}")
    println(s"   - Type: ${report.missingPlaceType}")

    println(s"\n3. Registros inválidos detectados:")
    println(s"   - Polaridades malformadas o fuera de rango [1-5]: ${report.invalidPolarity}")

    println("\n4. Distribuciones clave:")
    println("   - Polarity:")
    // sortBy(_._1) ordena los resultados (ej. 1, 2, 3, 4, 5, Ausente)
    report.polarityDistribution.toSeq.sortBy(_._1).foreach { case (k, v) => println(s"     $k -> $v") }
    
    println("   - Type:")
    report.typeDistribution.toSeq.sortBy(_._1).foreach { case (k, v) => println(s"     $k -> $v") }
    println("=" * 60)
  }
}