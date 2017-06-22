/*
 * Copyright (c) 2017. Astraea, Inc. All rights reserved.
 */
import geotrellis.raster.histogram.Histogram

object Utils {
  implicit class AsciiHistogram[T <: AnyVal : Numeric](hist: Histogram[T]) {
    def ascii(width: Int = 80) = {
      val labels = hist.values
      val counts = labels.map(hist.itemCount)
      val maxCount = counts.max.toFloat
      val maxLabelLen = labels.map(_.toString.length).max
      val maxCountLen = counts.map(c ⇒ f"$c%,d".length).max
      val fmt = s"%${maxLabelLen}s: %,${maxCountLen}d | %s"
      val barlen = width - fmt.format(0, 0, "").length

      val lines = for {
        (l, c) ← labels.zip(counts)
      } yield {
        val width = (barlen * (c/maxCount)).round
        val bar = "*" * width
        fmt.format(l, c, bar)
      }

      lines.mkString("\n")
    }
  }
}
