import geotrellis.raster._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.io.geotiff.reader.GeoTiffReader
import Utils._
import geotrellis.raster.render.ColorRamps
import  geotrellis.raster.mapalgebra.local._

/**
  * Created by jnachbar on 6/22/17.
  */

object TrellisTest {
  def main(args: Array[String]): Unit = {
    val path1 = "/Users/jnachbar/Downloads/LC80160342016111LGN00_B2.TIF"
    val path2 = "/Users/jnachbar/Downloads/LC80160342016111LGN00_BQA.TIF"
    val tile = GeoTiffReader.readSingleband(path1).tile
    val naTile = tile.convert(tile.cellType.withNoData(Some(0)))
    val qa = GeoTiffReader.readSingleband(path2).tile
    val mean = naTile.histogram.mean().get
    val stdev = naTile.histogram.statistics().get.stddev
    val qaBit = qa.map(value => if(value == 20512) 1 else 0)
//
//    val maskTile: Tile = naTile.localMask(qaBit, 1, NODATA)
//    maskTile.renderPng(ColorRamps.HeatmapBlueToYellowToRedSpectrum).write(s"/Users/jnachbar/Documents/Pictures/no_water.png")
//    val tiletimes2 = tile * tile + 3
//    def clip(value: Double, mean: Double, stdev: Int, devs: Int): Unit ={
//      if(Math.abs(mean - value) > (stdev * devs)) doubleNODATA else value
//
//
//    }

//    val clipTile: Tile = tile.mapDouble(value => if(Math.abs(mean - value) > (stdev * 1)) doubleNODATA else value)
//    println(clipTile.histogram.ascii())

    val splitLayout = new TileLayout(2, 2, (tile.cols - 1)/2, (tile.rows - 1)/2)
    val splits: Array[Tile] = tile.split(splitLayout)
    var time = System.currentTimeMillis()
    var i = 0
    time = System.currentTimeMillis()
    for(i <- 0 to splits.length - 1) {
      splits(i).renderPng(ColorRamps.HeatmapBlueToYellowToRedSpectrum).write(s"/Users/jnachbar/Documents/Pictures/$time$i.png")
    }
  }
}
