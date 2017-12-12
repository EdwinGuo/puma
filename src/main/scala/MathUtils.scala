package Puma

import scala.math.log10

object MathUtils {

  def logBaseXInt(x: Double, base: Int): Int = if (x == 0) 0 else (log10(x) / log10(base)).toInt

  def ceilLog2(x: Double) : Int = Math.ceil(log10(x) / log10(2d)).toInt

  /**
    * Find a bin for the given number
    */
  def entropyBin(value: Double, cuts: Array[Double]) : Int = {
    cuts.indexWhere(cut => value <= cut)
  }
}
