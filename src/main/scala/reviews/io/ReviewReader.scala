package reviews.io

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.security.MessageDigest
import scala.util.Try
import reviews.domain.ReviewRecord

object ReviewReader {

  def read(sc: SparkContext, path: String): RDD[ReviewRecord] = {
    val spark = SparkSession.builder().config(sc.getConf).getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("multiLine", "true")
      .option("escape", "\"")
      .csv(path)

    df.rdd.zipWithIndex().map { case (row, index) =>
      val sourceOrder = index.toLong 
      
   
      val title = Try(row.getAs[String]("Title")).toOption.flatMap(Option(_)).map(_.trim).getOrElse("")
      val review = Try(row.getAs[String]("Review")).toOption.flatMap(Option(_)).map(_.trim).getOrElse("")

      def getOptCol(colName: String): Option[String] = {
        Try(row.getAs[String](colName)).toOption.filter(v => v != null && v.trim.nonEmpty)
      }

      val town = getOptCol("Town")
      val region = getOptCol("Region")
      val placeType = getOptCol("Type")

      // none solo si esta ausente. Si es invslido se mapea usando la misma opcion
      val polarity = getOptCol("Polarity").map { p =>
        try {
          val doubleVal = p.toDouble
          if (doubleVal == Math.floor(doubleVal)) doubleVal.toInt
          else -1 // Bandera para valores presentes pero con formato decimal inválido
        } catch {
          case _: NumberFormatException => -1 // Bandera para valores inparseables
        }
      }

      val rawString = row.toSeq.map(v => if (v == null) "" else v.toString).mkString(",")
      val id = MessageDigest.getInstance("MD5")
        .digest(s"$sourceOrder-$rawString".getBytes("UTF-8"))
        .map("%02x".format(_)).mkString

      ReviewRecord(id, sourceOrder, title, review, town, region, placeType, polarity)
    }
  }
}