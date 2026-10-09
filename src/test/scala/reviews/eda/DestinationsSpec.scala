package analytics

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reviews.domain.ReviewRecord

class DestinationsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("DestinationsSpec")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) sc.stop()
  }

  // Mock data estratégico:
  // - "Guanajuato" concentra 4 registros (volumen dominante) con tipos y
  //   polaridades mixtas para validar los cruces.
  // - "Pueblito" tiene un único registro (muestra unitaria) para forzar
  //   una proporción exacta y pequeña, justificando el aviso de significancia.
  private def sampleRecords(): Seq[ReviewRecord] = Seq(
    ReviewRecord("r-1", 0L, "", "Review 1", Some("Guanajuato"), None, Some("Hotel"), Some(1)),
    ReviewRecord("r-2", 1L, "", "Review 2", Some("Guanajuato"), None, Some("Hotel"), Some(1)),
    ReviewRecord("r-3", 2L, "", "Review 3", Some("Guanajuato"), None, Some("Restaurante"), Some(-1)),
    ReviewRecord("r-4", 3L, "", "Review 4", Some("Guanajuato"), None, Some("Restaurante"), Some(0)),
    ReviewRecord("r-5", 4L, "", "Review 5", Some("Pueblito"), None, Some("Atraccion"), Some(1))
  )

  private def sampleRDD(): RDD[ReviewRecord] = sc.parallelize(sampleRecords())

  private val total: Long = 5L

  "frequencyByTown" should "contar correctamente el volumen dominante y la muestra unitaria" in {
    val result = Destinations.frequencyByTown(sampleRDD()).collect().toMap

    result("Guanajuato") shouldBe 4L
    result("Pueblito") shouldBe 1L
    result.size shouldBe 2
  }

  "proportionByTown" should "calcular proporciones matemáticas exactas" in {
    val result = Destinations.proportionByTown(sampleRDD(), total).collect().toMap

    // 4/5 = 0.8 exacto; valida el destino de volumen dominante.
    result("Guanajuato") shouldBe 0.8 +- 1e-9

    // 1/5 = 0.2 exacto; valida el caso de muestra unitaria que sustenta
    // el aviso de significancia estadística.
    result("Pueblito") shouldBe 0.2 +- 1e-9
  }

  it should "lanzar una excepción si el total es cero o negativo" in {
    an[IllegalArgumentException] should be thrownBy {
      Destinations.proportionByTown(sampleRDD(), 0L).collect()
    }
  }

  "crossByTownAndType" should "reflejar los conteos exactos por combinación destino-tipo" in {
    val result = Destinations.crossByTownAndType(sampleRDD()).collect().toMap

    result(("Guanajuato", "Hotel")) shouldBe 2L
    result(("Guanajuato", "Restaurante")) shouldBe 2L
    result(("Pueblito", "Atraccion")) shouldBe 1L
    result.size shouldBe 3
  }

  "crossByTownAndPolarity" should "reflejar los conteos exactos por combinación destino-polaridad" in {
    val result = Destinations.crossByTownAndPolarity(sampleRDD()).collect().toMap

    result(("Guanajuato", 1)) shouldBe 2L
    result(("Guanajuato", -1)) shouldBe 1L
    result(("Guanajuato", 0)) shouldBe 1L
    result(("Pueblito", 1)) shouldBe 1L
    result.size shouldBe 4
  }
}
