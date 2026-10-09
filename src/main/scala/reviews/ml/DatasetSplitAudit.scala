package reviews.ml

import org.apache.spark.{SparkConf, SparkContext}
import reviews.io.ReviewReader

object DatasetSplitAudit {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Error: Faltan argumentos.")
      System.err.println("Uso correcto: sbt \"runMain reviews.ml.DatasetSplitAudit <ruta-al-CSV>\"")
      System.exit(1)
    }

    val path = args(0)
    val conf = new SparkConf().setAppName("RestMex-DatasetSplitAudit").setMaster("local[*]")
    val sc = new SparkContext(conf)

    try {
      // ingesta a traves del reviewreader
      val records = ReviewReader.read(sc, path)
      
      // particion con el nuevo contrato de interfaz
      val splits = DatasetSplitter.split(records, seed = 42L)
      val report = splits.report
      
      // reporte
      println("=" * 60)
      println("REPORTE DE AUDITORÍA DE PARTICIÓN Y CODIFICACIÓN")
      println("=" * 60)
      println(s"Método: ${report.method}")
      println(s"Semilla: ${report.seed}")
      println(s"Total de registros leídos: ${report.totalRecords}")
      println("-" * 60)
      
      Seq("train", "validation", "holdout").foreach { partName =>
        val p = report.partitions(partName)
        val pct = (p.records.toDouble / report.totalRecords) * 100
        println(f"Partición: $partName%-10s | Registros: ${p.records}%7d ($pct%5.2f%%)")
        println(s"  Type     -> Faltantes: ${p.typeLabels.missing}, Inválidos: ${p.typeLabels.invalid}")
        println(s"              Conteos por Clase: ${p.typeLabels.byClass}")
        println(s"  Polarity -> Faltantes: ${p.polarityLabels.missing}, Inválidos: ${p.polarityLabels.invalid}")
        println(s"              Conteos por Clase: ${p.polarityLabels.byClass}\n")
      }
      
      println("-" * 60)
      println("AUDITORÍA DE DUPLICADOS (Agrupación por Llave Canónica)")
      println(s"- Grupos con texto duplicado: ${report.duplicateTextGroups}")
      println(s"- Grupos duplicados con Type contradictorio: ${report.duplicateTypeLabelConflictGroups}")
      println(s"- Grupos duplicados con Polarity contradictoria: ${report.duplicatePolarityLabelConflictGroups}")
      println(s"- Grupos duplicados repartidos en varias particiones: ${report.duplicateGroupsAcrossPartitions}")
      println("=" * 60)

    } catch {
      case e: Exception => 
        System.err.println(s"Error durante la ejecución: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      sc.stop()
    }
  }
}