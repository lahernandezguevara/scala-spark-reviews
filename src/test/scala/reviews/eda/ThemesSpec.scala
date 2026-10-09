package reviews.eda

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.SparkSession
import _root_.reviews.domain.ReviewRecord

class ThemesSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  // Crear la sesión Spark compartida para las pruebas de Themes
  implicit val spark: SparkSession = SparkSession.builder()
    .appName("ThemesSpec")
    .master("local[*]")
    .config("spark.ui.enabled", "false")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .getOrCreate()

  override protected def afterAll(): Unit = {
    // Detener Spark obligatoriamente al finalizar la suite para liberar la JVM
    spark.stop()
    super.afterAll()
  }

  "Themes (RDD-based)" should "conservar la suma de reseñas asignadas igual al total analizado" in {
    val data = Seq(
      ReviewRecord("id-1", 1L, "Titulo 1", "Excelente comida y servicio óptimo", None, None, None, Some(5)),
      ReviewRecord("id-2", 2L, "Titulo 2", "La habitación nunca estuvo limpia", None, None, None, Some(1)),
      ReviewRecord("id-3", 3L, "Titulo 3", "Sin duda un lugar maravilloso", None, None, None, Some(5)),
      ReviewRecord("id-4", 4L, "Titulo 4", "No me gustó nada la estancia", None, None, None, Some(1))
    )
    val rdd = spark.sparkContext.parallelize(data)

    val res = Themes.extractRecurringThemes(rdd, spark, numThemes = 2)
    // Aquí puedes agregar tus assertions para validar las pruebas requeridas
  }

  it should "preservar acentos y negaciones clave (no, nunca, sin)" in {
    // Tu prueba unitaria de preprocesamiento
  }

  it should "ser reproducible con la misma semilla" in {
    // Tu prueba unitaria de reproducibilidad con semilla fija
  }
}