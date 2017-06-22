import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import geotrellis.spark._
import geotrellis.spark.io.hadoop._
import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.render.ColorRamps
import geotrellis.vector.ProjectedExtent
import org.apache.spark.rdd.RDD

/**
  * Created by jnachbar on 6/20/17.
  */
object RasterFun {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .appName("raster spark")
      .master("local[*]")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    import sparkSession.implicits._

    val b2: RDD[(ProjectedExtent, Tile)] = sc.hadoopGeoTiffRDD("file:///Users/jnachbar/Downloads/LC80160342016111LGN00_B2.TIF")

    val b2justTile: RDD[Tile] = b2.map(_._2)


    //val histo: IntHistogram = FastMapHistogram.fromTile(b2justTile.first())

    val histo = b2.histogramExactInt
    println(histo.statistics())
    //  b2.map(_._2).zipWithIndex.foreach({
    //    case (tile: Tile, index: Long) =>
    //      println(index)
    //      tile.renderPng(ColorRamps.HeatmapBlueToYellowToRedSpectrum).write(s"/tmp/myfile$index.png")
    //  })
  }
}
