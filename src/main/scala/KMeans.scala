import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.render.ColorRamps
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD
import Utils._

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
  //add the index of the tile to the output sequence
  //why am I doing this? don't I end up with only one feature vector anyway?
  //maybe for reconstruction
  def tilesToKeyPx(t1: (Tile), t2: (Tile), t3: (Tile)) = {
    for {r <- 0 until t1.rows; c <- 0 until t1.cols}
        yield(Seq(c, r, t1.get(c, r), t2.get(c, r), t3.get(c, r)))
  }
  def expandTuple(pe: ProjectedExtent, t: ((Tile, Tile), Tile)) = (pe, t._1._1,  t._1._2, t._2)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("IrisSpark")
    val sparkSession = SparkSession.builder
      .appName("spark session example")
      .master("local[*]")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val blueTIF: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD("file:///Users/jnachbar/Downloads/LC80160342016111LGN00_B2.TIF")
    val greenTIF: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD("file:///Users/jnachbar/Downloads/LC80160342016111LGN00_B3.TIF")
    val redTIF : RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD("file:///Users/jnachbar/Downloads/LC80160342016111LGN00_B4.TIF")
//    val blue: RDD[Tile] = blueTIF.map(_._2)
//    val green: RDD[Tile] = greenTIF.map(_._2)
//    val red: RDD[Tile] = redTIF.map(_._2)
    //I need an RDD with PE,
    val peRDD = (blueTIF join greenTIF join redTIF)
    val expandRDD =
//    val threeBand = blue.zip(red).zip(green)
//    val vectorBands = threeBand.map(value => expandTuple(value))
    //create function to explode tiles into pixel/key pairs
  }
}
