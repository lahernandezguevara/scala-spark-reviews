package reviews.eda

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.{SparkConf, SparkContext}
import reviews.domain.ReviewRecord

class QualitySpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("TestQuality").setMaster("local[1]")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) sc.stop()
  }

  "Quality.analyze" should "contabilizar correctamente los valores ausentes e inválidos" in {
    val records = Seq(
      ReviewRecord("1", 1L, "Buen lugar", "Ok", Some("Tulum"), Some("QR"), Some("Hotel"), Some(5)),
      ReviewRecord("2", 2L, "", "", None, Some("QR"), None, None), // Faltantes
      ReviewRecord("3", 3L, "", "Limpio", Some("Valladolid"), None, Some("Hotel"), Some(99)), // Inválido (rango)
      ReviewRecord("4", 4L, "Normal", "Sí", Some("Cancun"), Some("QR"), Some("Restaurant"), Some(-1)) // Inválido (parse)
    )
    val rdd = sc.parallelize(records)
    val report = Quality.analyze(rdd)

    report.totalRecords shouldEqual 4
    report.missingTitle shouldEqual 2    
    report.missingReview shouldEqual 1   
    report.missingPolarity shouldEqual 1 // Fila 2
    report.invalidPolarity shouldEqual 2 // Filas 3 y 4
  }
}