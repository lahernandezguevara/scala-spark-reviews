package reviews.eda

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SparkSession
import _root_.reviews.domain.ReviewRecord

object Themes {

  val NegacionesCruciales: Set[String] = Set("no", "nunca", "sin")

  // Lista ampliada de stopwords (palabras vacías) en español para evitar que dominen los clústeres
  val StopWords: Set[String] = Set(
    "que", "de", "a", "el", "la", "en", "y", "un", "es", "se", "no", "es", "por", 
    "con", "su", "para", "como", "las", "del", "los", "le", "lo", "o", "sus", 
    "fue", "son", "este", "ha", "si", "su", "ya", "todo", "esta", "entre", "cuando", 
    "muy", "sin", "sobre", "también", "me", "hasta", "hay", "donde", "quien", 
    "desde", "todo", "nos", "durante", "todos", "uno", "les", "ni", "contra", 
    "otros", "ese", "eso", "ante", "ellos", "e", "esto", "mí", "antes", "algunos", 
    "qué", "unos", "yo", "otro", "otras", "otra", "él", "tanto", "esa", "estos", 
    "mucho", "quienes", "nada", "muchos", "cual", "poco", "ella", "estar", "estas", 
    "algunas", "algo", "nosotros", "mi", "mis", "tú", "te", "ti", "tu", "tus", 
    "ellas", "nosotras", "vosotros", "vosotras", "os", "mío", "mía", "míos", "mías", 
    "tuyo", "tuya", "tuyos", "tuyas", "suyo", "suya", "suyos", "suyas", "nuestro", 
    "nuestra", "nuestros", "nuestras", "vuestro", "vuestra", "vuestros", "vuestras", 
    "esos", "esas", "estas", "estos", "está", "están", "estaba", "estaban", "estado", "una", "era", "pero", "más"
  )

  def extractRecurringThemes(
      reviewsRdd: RDD[ReviewRecord],
      spark: SparkSession,
      numThemes: Int = 3,
      seed: Long = 42L
  ): List[ThemeSummary] = {
    
    // 1. Limpieza, tokenización y filtrado de stopwords (conservando negaciones críticas)
    val tokensRdd = reviewsRdd.map { record =>
      val tokens = record.review.toLowerCase
        .split("""[^\p{L}\p{Nd}]+""")
        .filter(t => (t.length > 2 && !StopWords.contains(t)) || NegacionesCruciales.contains(t))
      (record.sourceOrder, record.review, tokens.toSeq)
    }

    // 2. Filtrar reseñas válidas y calcular total analizado
    val validasRdd = tokensRdd.filter(_._3.nonEmpty)
    val totalAnalizado = validasRdd.count()

    if (totalAnalizado == 0) return List.empty[ThemeSummary]

    // 3. TF-IDF con MLlib
    val termDocumentRdd: RDD[Seq[String]] = validasRdd.map(_._3)
    val hashingTF = new HashingTF(10000)
    val tfRdd: RDD[Vector] = hashingTF.transform(termDocumentRdd)
    tfRdd.cache()

    val idfModel = new IDF().fit(tfRdd)
    val tfIdfRdd: RDD[Vector] = idfModel.transform(tfRdd)

    // 4. KMeans con semilla fija para reproducibilidad determinista
    val indexedDataRdd = validasRdd.zip(tfIdfRdd)
    val vectorsOnly = indexedDataRdd.values
    val kmeansModel = KMeans.train(vectorsOnly, numThemes, 20, "random", seed)

    // 5. Asignar cada reseña a su clúster correspondiente
    val clusteredRdd = indexedDataRdd.map { case (recordInfo, vector) =>
      val clusterId = kmeansModel.predict(vector)
      (clusterId, recordInfo)
    }
    clusteredRdd.cache()

    // 6. Construir el resumen por cada tema (clúster)
    val clusterIds = (0 until numThemes).toList

    val summaries = clusterIds.map { cid =>
      val clusterItems = clusteredRdd.filter(_._1 == cid).values.cache()
      val clusterSize = clusterItems.count()

      // Extraer términos más frecuentes ignorando las stopwords
      val topTerms = clusterItems
        .flatMap(_._3)
        .filter(word => !StopWords.contains(word))
        .map(word => (word, 1L))
        .reduceByKey(_ + _)
        .sortBy(_._2, ascending = false)
        .take(5)
        .map(_._1)
        .toSeq

      val evidenceOrders = clusterItems
        .map(_._1)
        .sortBy(identity)
        .take(3)
        .toSeq

      ThemeSummary(
        name = s"Tema ${cid + 1}",
        representativeTerms = topTerms,
        distinctReviewsCount = clusterSize,
        denominator = totalAnalizado,
        evidenceSourceOrders = evidenceOrders
      )
    }

    summaries
  }
}