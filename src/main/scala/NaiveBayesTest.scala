import org.apache.spark.ml.Pipeline
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.{LabeledPoint, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}

/**
  * Created by jnachbar on 6/20/17.
  */
object NaiveBayesTest {
  def main(args: Array[String]): Unit = {
    //creating a new SparkSession
    val conf = new SparkConf().setMaster("local[*]").setAppName("IrisSpark")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()
    //specifying the path to my file
    val path = "/Users/jnachbar/Downloads/iris.csv"
    val data: DataFrame = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
    //prints the metadata associated with the dataframe
    data.printSchema()
    //create a transformer which will aggregate features into a single vector
    val featVec = new VectorAssembler()
      .setInputCols(Array("SepalLength", "SepalWidth", "PetalLength", "PetalWidth"))
      .setOutputCol("features")
    //    val trainFeatures = assembler.transform(dataOne)
    //    val testFeatures = assembler.transform(dataTwo)
    //creates a transformer which indexes the labels from the dataset and saves the key as an attribute
    val nameIndexer = new StringIndexer()
      .setInputCol("Name")
      .setOutputCol("nameIndex")
      .fit(data)
    //select only the labels and the feature vectors and creates a RDD out of them
    val indexData = featVec.transform(nameIndexer.transform(data)).select("Name", "features")
    //splits my dataset into training and testing data
    val Array(train, test) = indexData.randomSplit(Array(.8, .2))
    val nb = new NaiveBayes().fit(train)
    //nb.run()
  }
}
