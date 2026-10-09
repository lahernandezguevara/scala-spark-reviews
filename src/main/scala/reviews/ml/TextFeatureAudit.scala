// src/main/scala/reviews/ml/TextFeatureAudit.scala
package reviews.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import reviews.io.ReviewReader

object TextFeatureAudit {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Uso: sbt \"runMain reviews.ml.TextFeatureAudit <ruta-al-CSV>\"")
      sys.exit(1)
    }
    val path = args(0)

    val conf = new SparkConf()
      .setAppName("TextFeatureAudit")
      .setIfMissing("spark.master", "local[*]")
    val sc = new SparkContext(conf)

    val records = ReviewReader.read(sc, path).persist(StorageLevel.MEMORY_AND_DISK)
    var splits: Option[DatasetSplits] = None

    try {
      val s = DatasetSplitter.split(records)
      splits = Some(s)

      val features = TextFeatures.fitAndTransform(s)
      val r = features.report

      println("=" * 64)
      println("AUDITORÍA DE FEATURES TF-IDF")
      println("=" * 64)
      println("Comando        : sbt \"runMain reviews.ml.TextFeatureAudit <ruta-al-CSV>\"")
      println(s"Spark          : ${sc.version}")
      println(s"Total registros (split): ${s.report.totalRecords}")
      println(s"Dimensión de features (HashingTF.numFeatures): ${r.numFeatures}")
      println(s"Documentos usados para ajustar IDF: ${r.idfFitDocumentCount}")
      println()

      def show(name: String, p: FeatureSplitReport): Unit = {
        println(s"[$name]")
        println(s"  filas de entrada          : ${p.inputCount}")
        println(s"  filas de salida           : ${p.outputCount}")
        println(s"  textos sin tokens         : ${p.emptyTokenCount}")
        println(s"  etiquetas Type válidas    : ${p.validTypeLabelCount}")
        println(s"  etiquetas Polarity válidas: ${p.validPolarityLabelCount}")
        println(s"  outputCount == inputCount : ${p.outputCount == p.inputCount}")
      }
      show("train", r.train)
      show("validation", r.validation)
      show("holdout", r.holdout)
      println()

      val trainSplitCount = s.report.partitions("train").records
      val idfOnlyTrain =
        r.idfFitDocumentCount == r.train.inputCount &&
          r.idfFitDocumentCount == trainSplitCount
      val countsOk = Seq(r.train, r.validation, r.holdout).forall(p => p.inputCount == p.outputCount)

      println(s"IDF ajustado únicamente con train: $idfOnlyTrain " +
        s"(documentos IDF = ${r.idfFitDocumentCount}, train = $trainSplitCount)")
      println(s"Conservación de filas en las tres particiones: $countsOk")
      println("=" * 64)

      if (!idfOnlyTrain || !countsOk) {
        throw new IllegalStateException("La auditoría detectó inconsistencias en los conteos.")
      }
    } finally {
      try {
        splits.foreach { s =>
          s.train.unpersist(blocking = false)
          s.validation.unpersist(blocking = false)
          s.holdout.unpersist(blocking = false)
        }
        records.unpersist(blocking = false)
      } finally {
        sc.stop()
      }
    }
  }
}