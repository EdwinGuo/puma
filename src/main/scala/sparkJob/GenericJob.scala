package sparkJob

import org.apache.commons.lang.SystemUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD

trait GenericJob[I,O]{
  case class SaveParams(df: O, parameters: SparkParameters, partitions: Seq[String])

  def run(parameter: SparkParameters): Unit = {
    implicit val spark = {
      var ss = SparkSession.builder
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .appName(sparkParameters.parser)
      if (SystemUtils.IS_OS_WINDOWS) ss = ss.master("local[*]")
      ss
    }
    implicit val sc = spark.sparkContext
    implicit val sparkParameters = parameter

    val df = read(parameter)
    val seqParameters = transform(df)
    save(seqParameters:_*)
    // stop the resource
    spark.stop()
  }

  def read(params: SparkParameters)(implicit spark: SparkSession): I

  def transform(df: I)(implicit spark: SparkSession, sparkParams: SparkParameters): Seq[SaveParams]

  def save(p: SaveParams*): Unit

}
