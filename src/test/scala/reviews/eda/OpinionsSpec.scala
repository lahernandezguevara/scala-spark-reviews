package reviews.eda

import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import reviews.domain.ReviewRecord

class OpinionsSpec extends AnyFunSuite with BeforeAndAfterAll {

  @transient var sc: SparkContext = _

  override def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setAppName("OpinionsTestRefactored")
      .setMaster("local[2]")
    sc = new SparkContext(conf)
  }

  override def afterAll(): Unit = {
    if (sc != null) sc.stop()
  }

  test("Debe evaluar exclusiones (0, 6, 3, None) correctamente") {
    val data = Seq(
      ReviewRecord("1", 1L, "", "malo", None, None, None, Some(0)),
      ReviewRecord("2", 2L, "", "malo", None, None, None, Some(6)),
      ReviewRecord("3", 3L, "", "normal", None, None, None, Some(3)),
      ReviewRecord("4", 4L, "", "nada", None, None, None, None)
    )
    val stats = Opinions.analyze(sc.parallelize(data))
    assert(stats.invalidOrMissingCount == 3)
    assert(stats.neutralCount == 1)
    assert(stats.complaintsCount == 0)
    assert(stats.strengthsCount == 0)
  }

  test("Debe contar unigramas y bigramas repetidos 1 vez por reseña y conservar 'no', 'nunca', 'sin'") {
    val data = Seq(
      // "hotel limpio" se repite, debe contar 1 vez. Contiene "sin" y "nunca"
      ReviewRecord("1", 1L, "", "hotel limpio hotel limpio sin ruido nunca", None, None, None, Some(5)),
      // Contiene "no" y la stop word "es"
      ReviewRecord("2", 2L, "", "no es bueno", None, None, None, Some(1))
    )
    val stats = Opinions.analyze(sc.parallelize(data))

    val strMap = stats.topStrengths.toMap
    assert(strMap.getOrElse("hotel", 0) == 1)
    assert(strMap.getOrElse("limpio", 0) == 1)
    assert(strMap.getOrElse("hotel limpio", 0) == 1)
    assert(strMap.getOrElse("sin", 0) == 1)
    assert(strMap.getOrElse("nunca", 0) == 1)

    val compMap = stats.topComplaints.toMap
    assert(compMap.getOrElse("no", 0) == 1)
    assert(compMap.getOrElse("bueno", 0) == 1)
    assert(!compMap.contains("es")) // Stop word eliminada
  }

  test("Debe resolver empates de manera determinista (-count, term alfabético)") {
    val data = Seq(
      ReviewRecord("1", 1L, "", "zapato", None, None, None, Some(1)),
      ReviewRecord("2", 2L, "", "arbol", None, None, None, Some(1)),
      ReviewRecord("3", 3L, "", "barco", None, None, None, Some(1))
    )
    val stats = Opinions.analyze(sc.parallelize(data))

    // Todos tienen frecuencia 1. Orden alfabético forzado: arbol, barco, zapato.
    val unigramsOnly = stats.topComplaints.filter(x => !x._1.contains(" ")).take(3)
    assert(unigramsOnly == Seq(("arbol", 1), ("barco", 1), ("zapato", 1)))
  }

  test("Debe procesar correctamente caracteres unicode (ej. diéresis)") {
    val data = Seq(ReviewRecord("1", 1L, "", "pingüino", None, None, None, Some(5)))
    val stats = Opinions.analyze(sc.parallelize(data))
    assert(stats.topStrengths.contains(("pingüino", 1)))
  }
}