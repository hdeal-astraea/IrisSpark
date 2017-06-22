import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by jnachbar on 6/20/17.
  */
object KMeansTest {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("IrisSpark")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()
    val path = "/Users/jnachbar/Downloads/iris.csv"
    val data: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
    val kmeans = new KMeans()
      .setK(3)
      .setSeed(2)
  }
}
