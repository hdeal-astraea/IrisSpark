import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession, types}
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.render.ColorRamps
import geotrellis.vector.{Extent, ProjectedExtent}
import org.apache.spark.rdd.RDD
import Utils._
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.types.DateType._
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

/**
  * Created by jnachbar on 6/20/17.
  */

//process:
//1. Collect data into one RDD
//2. Expand the RDD structure to make it easier to map
//3. Get a sequence of sequences with (index, row, column, data1, data2, data3)
//4. Turn the first three values into an identifier and the last three into a feature vector
//5. Turn the whole structure into a dataframes
//6. Run K-Means
//7. Reconstruct the image using your predefined index

object KMeans {
//  add the index of the tile to the output sequence
//  why am I doing this? don't I end up with only one feature vector anyway?
//  maybe for reconstruction
  def tilesToKeyPx(pe: ProjectedExtent, t1: (Tile), t2: (Tile), t3: (Tile)) = {
    for {
      r <- 0 until t1.rows
      c <- 0 until t1.cols
      v = t1.get(c, r)
      if v > 0
    } yield(pe.extent, c, r, Vectors.dense(t1.get(c, r), t2.get(c, r), t3.get(c, r)))
          //r, t1.get(c, r), t2.get(c, r), t3.get(c, r))
  }

  def expandTuple(t: ((Tile, Tile), Tile)) = (t._1._1,  t._1._2, t._2)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("IrisSpark")
    val sparkSession = SparkSession.builder
      .appName("KMeans")
      .master("local[*]")
      .getOrCreate()

    import sparkSession.implicits._

    val sc = sparkSession.sparkContext
    val blueTIF: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD("file:///Users/jnachbar/Downloads/LC80160342016111LGN00_B2.TIF")
    val greenTIF: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD("file:///Users/jnachbar/Downloads/LC80160342016111LGN00_B3.TIF")
    val redTIF : RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD("file:///Users/jnachbar/Downloads/LC80160342016111LGN00_B4.TIF")
    val blue: RDD[(ProjectedExtent, Tile)] = blueTIF
      .split(256, 256)
    val green: RDD[(ProjectedExtent, Tile)] = greenTIF
      .split(256, 256)
    val red: RDD[(ProjectedExtent, Tile)] = redTIF
      .split(256, 256)

    val peRDD = (blue.zip(green).zip(red))
    val expandRDD = peRDD.map{
      case((t1,t2),t3) => (t1, t2, t3)
    }

    val tupleRDD = expandRDD.flatMap{ case ((pe1: ProjectedExtent, value1: Tile),
    (pe2: ProjectedExtent, value2: Tile),
    (pe3: ProjectedExtent, value3: Tile)) => tilesToKeyPx(pe1, value1, value2, value3) }
    val df = tupleRDD.toDF("pe", "col", "row", "features").repartition(200)
    //val schemaString = "Col Row"
      //"Tile1 Tile2 Tile3"
    //val labels = schemaString.split(" ").map(name => StructField(name, types.IntegerType, nullable = true))
    //val schema = StructType(labels)
    //make it so that I go through every tuple, not just every sequence
    //val rowSeq = tupleRDD.first().map(sequence => (sequence._1, sequence._2))
    //val rowRDD = sc.parallelize(rowSeq.map(value => Row(value._1, value._2)))
      //sequence.apply(2), sequence.apply(3),
      //sequence.apply(4), sequence.apply(5)))
    //val rawData = sparkSession.createDataFrame(rowRDD, schema)
    //rawData.show(5)
    //create function to explode tiles into pixel/key pairs
    val kmeans = new KMeans().setFeaturesCol("features").setK(6)
    val model = kmeans.fit(df)
    val preds = model.transform(df)
    preds.groupBy("prediction").count().show()
    //start with an RDD with partitions based on one of the 900 possible extents

    //take a sequence and make it into an array
    def iterArray(seq: Seq[(Extent, Int, Int, Double)]): Array[Double] = {
      val valArr = Array.ofDim[Double](65025)
      for(i: Int <- 0 until 65025)
        valArr(i) = seq.apply(i)._4
      valArr
    }

    val predsRDD = preds.select("pe", "col", "row", "prediction")
      .as[(Extent, Int, Int, Double)].rdd
      .groupBy(_._1)
    val arrRDD = predsRDD.map(value => (value._1, iterArray(value._2.toSeq)))
    val arrTile = arrRDD.map(pair => (pair._1, ArrayTile(pair._2, 255, 255)))
    println(arrTile.first()._2.cols)
    println(arrTile.first()._2.rows)
  }
}
