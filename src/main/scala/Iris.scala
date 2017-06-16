import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils


object IrisSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("IrisSpark")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()
    val path = "/Users/jnachbar/Downloads/iris.csv"
    val data: DataFrame = sparkSession.read
      .option("header","true")
      .option("inferSchema", "true")
      .csv(path)
    data.printSchema()
    val split = data.randomSplit(Array(.5,.5))
    val dataOne = split(0)
    val dataTwo = split(1)
    dataOne.show()
    dataTwo.show()
    //dataOne.groupBy("SepalLength").pivot("SepalLength").mean("SepalLength")
    //dataOne.join(dataTwo, dataOne("Name"), "outer")
    (data.groupBy("Name").mean("SepalWidth")).show()
  }
}