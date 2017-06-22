import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}


object IrisSpark {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("IrisSpark")
    val sc = new SparkContext(conf)
    val sparkSession = SparkSession.builder
      .config(conf = conf)
      .appName("spark session example")
      .getOrCreate()

    import sparkSession.implicits._

    val path = "/Users/jnachbar/Downloads/iris.csv"
    val data: DataFrame = sparkSession.read
      .option("header","true")
      .option("inferSchema", "true")
      .csv(path)
    data.printSchema()
    val Array(train, test) = data.randomSplit(Array(.8,.2))
    val featVec = new VectorAssembler()
      .setInputCols(Array("SepalLength", "SepalWidth", "PetalLength", "PetalWidth"))
      .setOutputCol("features")
//    val trainFeatures = assembler.transform(dataOne)
//    val testFeatures = assembler.transform(dataTwo)
    //need LabelIndexer
    val nameIndexer = new StringIndexer()
        .setInputCol("Name")
        .setOutputCol("nameIndex")
        .fit(data)
    val predConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predLabel")
        .setLabels(nameIndexer.labels)
    val tree = new DecisionTreeClassifier()
      .setLabelCol("nameIndex")
      .setFeaturesCol("features")
    val pipeline = new Pipeline()
      .setStages(Array(nameIndexer, featVec, tree, predConverter))
    val model = pipeline.fit(train)
    val preds = model.transform(test)
    preds.select("Name", "predLabel").show(10)
    val evaluator = new MulticlassClassificationEvaluator()
        .setLabelCol("nameIndex")
        .setPredictionCol("prediction")
        .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(preds)
    println("accuracy: " + (accuracy * 100) + "%")

    preds.printSchema()
    val notDataFrame = data.as[IrisData].rdd
    println(notDataFrame.map(_.SepalWidth).mean())

    //notDataFrame.map(_.)
    //dataOne.join(dataTwo, dataOne("Name"), "outer")
    //(data.groupBy("Name").mean("SepalWidth", "SepalLength")).show()

  }

  case class IrisData(SepalWidth: Double, SepalLength: Double, PetalLength: Double, PetalWidth: Double, Name: String)
}