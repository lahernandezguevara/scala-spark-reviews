package reviews

import org.apache.spark.{SparkConf, SparkContext}
import reviews.io.ReviewReader
import reviews.eda.Quality

object Main {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RestMexReviews").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val path = "/mnt/c/Users/elcal/Downloads/Rest-Mex_2025_Train_DataSet/Rest-Mex_2025_train.csv"
    
    val reviewsRDD = ReviewReader.read(sc, path)
    Quality.analyze(reviewsRDD)
    sc.stop()
  }
}