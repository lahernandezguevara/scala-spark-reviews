package reviews.eda

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import reviews.domain.ReviewRecord

class DistributionsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("DistributionsSpec")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) sc.stop()
  }

  "Distributions.analyze" should "calcular correctamente conteos, porcentajes y denominadores con clases válidas" in {
    val records = Seq(
      ReviewRecord("1", 1L, "", "", None, None, Some("Restaurant"), Some(5)),
      ReviewRecord("2", 2L, "", "", None, None, Some("Restaurant"), Some(5)),
      ReviewRecord("3", 3L, "", "", None, None, Some("Attractive"), Some(4)),
      ReviewRecord("4", 4L, "", "", None, None, Some("Hotel"), Some(1))
    )
    val rdd = sc.parallelize(records)
    val report = Distributions.analyze(rdd)

    report.totalRecords shouldBe 4
    
    // Type Validations (Denominador = 4)
    val typeStats = report.typeReport.validStats
    typeStats.map(_.className) shouldBe Seq("Restaurant", "Attractive", "Hotel")
    typeStats.find(_.className == "Restaurant").get.count shouldBe 2
    typeStats.find(_.className == "Restaurant").get.percentage shouldBe 50.0
    typeStats.forall(_.denominator == 4) shouldBe true

    // Polarity Validations (Denominador = 4)
    val polStats = report.polarityReport.validStats
    polStats.map(_.className) shouldBe Seq("1", "2", "3", "4", "5")
    polStats.find(_.className == "5").get.count shouldBe 2
    polStats.find(_.className == "5").get.percentage shouldBe 50.0
    polStats.find(_.className == "2").get.count shouldBe 0
    polStats.forall(_.denominator == 4) shouldBe true
  }

  it should "separar las etiquetas ausentes e inválidas sin sumarlas a los denominadores válidos" in {
    val records = Seq(
      ReviewRecord("1", 1L, "", "", None, None, Some("Restaurant"), Some(5)),
      ReviewRecord("2", 2L, "", "", None, None, None, None), // Missing
      ReviewRecord("3", 3L, "", "", None, None, Some("Plaza"), Some(99)), // Invalid Type and Polarity
      ReviewRecord("4", 4L, "", "", None, None, Some("Hotel"), Some(-1))  // Invalid Polarity
    )
    val rdd = sc.parallelize(records)
    val report = Distributions.analyze(rdd)

    report.totalRecords shouldBe 4

    // Valid Type Denominator should be 2 ("Restaurant", "Hotel")
    report.typeReport.validStats.head.denominator shouldBe 2
    report.typeReport.missing.count shouldBe 1
    report.typeReport.invalid.count shouldBe 1
    report.typeReport.invalid.percentage shouldBe 25.0

    // Valid Polarity Denominator should be 1 (Some(5))
    report.polarityReport.validStats.head.denominator shouldBe 1
    report.polarityReport.missing.count shouldBe 1
    report.polarityReport.invalid.count shouldBe 2
    report.polarityReport.invalid.percentage shouldBe 50.0
  }

  it should "identificar las clases mayoritaria/minoritaria y calcular la razón de desbalance" in {
    val records = Seq(
      ReviewRecord("1", 1L, "", "", None, None, Some("Restaurant"), Some(1)),
      ReviewRecord("2", 2L, "", "", None, None, Some("Restaurant"), Some(1)),
      ReviewRecord("3", 3L, "", "", None, None, Some("Restaurant"), Some(1)),
      ReviewRecord("4", 4L, "", "", None, None, Some("Hotel"), Some(2))
      // Attractive queda implícitamente en 0, pero como hay 0 probaremos esto en la siguiente prueba.
      // Modificaremos un record para que Attractive tenga 1 y Hotel 2 para probar razón numérica.
    )
    val recordsAjustados = Seq(
      ReviewRecord("1", 1L, "", "", None, None, Some("Restaurant"), Some(1)),
      ReviewRecord("2", 2L, "", "", None, None, Some("Restaurant"), Some(1)),
      ReviewRecord("3", 3L, "", "", None, None, Some("Restaurant"), Some(1)),
      ReviewRecord("4", 4L, "", "", None, None, Some("Restaurant"), Some(1)),
      ReviewRecord("5", 5L, "", "", None, None, Some("Hotel"), Some(2)),
      ReviewRecord("6", 6L, "", "", None, None, Some("Hotel"), Some(2)),
      ReviewRecord("7", 7L, "", "", None, None, Some("Attractive"), Some(3))
    )
    val report = Distributions.analyze(sc.parallelize(recordsAjustados))

    val typeImbalance = report.typeReport.imbalance.get
    typeImbalance.majorityClass shouldBe "Restaurant" // 4
    typeImbalance.minorityClass shouldBe "Attractive" // 1
    typeImbalance.ratio shouldBe Some(4.0) 

    val polImbalance = report.polarityReport.imbalance.get
    polImbalance.majorityClass shouldBe "1" // 4
    polImbalance.minorityClass shouldBe "3" // 1
    polImbalance.ratio shouldBe Some(4.0) 
  }

  it should "manejar correctamente el desbalance cuando existen clases esperadas con conteo cero" in {
    // Solo enviamos reseñas de Restaurant. "Hotel" y "Attractive" tendrán 0 conteos.
    val records = Seq(
      ReviewRecord("1", 1L, "", "", None, None, Some("Restaurant"), Some(5)),
      ReviewRecord("2", 2L, "", "", None, None, Some("Restaurant"), Some(5))
    )
    val report = Distributions.analyze(sc.parallelize(records))

    val typeImbalance = report.typeReport.imbalance.get
    typeImbalance.majorityClass shouldBe "Restaurant"
    // Como Hotel y Attractive tienen 0, la minoritaria puede ser cualquiera de ellas.
    typeImbalance.minorityClass should not be "Restaurant"
    typeImbalance.ratio shouldBe None // Razón no definida por división por cero
    
    // Verificamos que sí existan en las estadísticas
    val typeStats = report.typeReport.validStats
    typeStats.find(_.className == "Hotel").get.count shouldBe 0
    typeStats.find(_.className == "Attractive").get.count shouldBe 0
  }

  it should "no producir NaN ni Infinity cuando no hay etiquetas válidas en absoluto" in {
    val records = Seq(ReviewRecord("1", 1L, "", "", None, None, None, None))
    val rdd = sc.parallelize(records)
    val report = Distributions.analyze(rdd)

    report.typeReport.validStats.forall(_.percentage == 0.0) shouldBe true
    report.typeReport.imbalance shouldBe None
    report.polarityReport.validStats.forall(_.percentage == 0.0) shouldBe true
    report.polarityReport.imbalance shouldBe None
  }
}