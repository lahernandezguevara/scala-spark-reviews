package reviews.io

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.{SparkConf, SparkContext}
import java.io.{File, PrintWriter}
import reviews.domain.ReviewRecord

class ReviewReaderSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = _
  var tempFilePath: String = _
  var tempFileMissingCols: String = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf().setAppName("TestReviewReader").setMaster("local[1]")
    sc = new SparkContext(conf)

    val csvContent =
      """Title,Review,Polarity,Town,Region,Type
        |"Buen lugar","Comida buena",5.0,Tulum,Quintana Roo,Restaurant
        |"","Título vacío",4,Bacalar,Quintana Roo,Attractive
        |"Saltos","Esta reseña tiene
        |un salto.",3,Valladolid,,Hotel
        |"Decimal falso","Se vuelve -1",5.9,Izamal,Yucatan,Hotel
        |"Fuera de rango","Se conserva el 99",99,Cancun,Quintana Roo,Restaurant""".stripMargin

    val tempFile = File.createTempFile("fake_reviews", ".csv")
    tempFile.deleteOnExit()
    new PrintWriter(tempFile) { write(csvContent); close() }
    tempFilePath = tempFile.getAbsolutePath

    val missingColsContent =
      """Title,Review,Town,Region
        |"Sin tipo","Faltan columnas",Tulum,QRoo""".stripMargin

    val tempMissing = File.createTempFile("missing_cols", ".csv")
    tempMissing.deleteOnExit()
    new PrintWriter(tempMissing) { write(missingColsContent); close() }
    tempFileMissingCols = tempMissing.getAbsolutePath
  }

  override def afterAll(): Unit = {
    if (sc != null) sc.stop()
  }

  "ReviewReader" should "garantizar IDs estables y sourceOrder lógico" in {
    val rdd1 = ReviewReader.read(sc, tempFilePath).collect()
    val rdd2 = ReviewReader.read(sc, tempFilePath).collect()
    rdd1.map(r => (r.id, r.sourceOrder)) shouldEqual rdd2.map(r => (r.id, r.sourceOrder))
    rdd1.map(_.sourceOrder).sorted shouldEqual Array(0L, 1L, 2L, 3L, 4L)
  }

  it should "conservar la reseña multilínea en su posición lógica" in {
    val records = ReviewReader.read(sc, tempFilePath).collect()
    val multilineReview = records.find(_.sourceOrder == 2).get

    multilineReview.title shouldEqual "Saltos"
    multilineReview.review shouldEqual "Esta reseña tiene\nun salto."
    multilineReview.town shouldEqual Some("Valladolid")
    multilineReview.placeType shouldEqual Some("Hotel")
  }

  it should "manejar títulos vacíos conservándolos como string vacío" in {
    val records = ReviewReader.read(sc, tempFilePath).collect()
    records.find(_.sourceOrder == 1).get.title shouldEqual ""
  }

  it should "conservar la información de polaridades inválidas sin volverlas None" in {
    val records = ReviewReader.read(sc, tempFilePath).collect()
    records.find(_.sourceOrder == 0).get.polarity shouldEqual Some(5)
    records.find(_.sourceOrder == 3).get.polarity shouldEqual Some(-1) // Malformado (5.9)
    records.find(_.sourceOrder == 4).get.polarity shouldEqual Some(99) // Fuera de rango
  }

  it should "asignar None únicamente cuando la columna de polaridad no existe" in {
    val records = ReviewReader.read(sc, tempFileMissingCols).collect()
    records.head.polarity shouldEqual None
  }
}
