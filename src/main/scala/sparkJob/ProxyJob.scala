package sparkJob

import parser.ProxyParser._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object ProxyJob extends GenericJob[RDD[String], DataFrame] {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def read(params: SparkParams)(implicit spark: SparkSession): RDD[String] = {
    spark.sparkContext.textFile(params.inPath)
  }

  override def transform(data: RDD[String])(implicit spark: SparkSession, params: SparkParams) = {
    import spark.implicits._

    val parsedAndConvertTimeRDD = RDDUtilJob.parseAndConvertTimeRDD(data)(BlueCoatParser)

    val invalidDF = RDDUtilJob.getInvalidDF(parsedAndConvertTimeRDD)

    val validRDD = RDDUtilJob.getValidRDD(parsedAndConvertTimeRDD)

    val validToDF = RDDUtilJob.createDFWithPartition[ProxySchema](validRDD, date_format($"orig_timestamp".substr(0,10), "yyyyMMdd"))

    val finalValidDF = castColumnsType(validToDF).drop($"time")

    val outputDF = Seq((finalValidDF, params, Seq(RhinoConstants.EventDateColumn)),
      (invalidDF, RDDUtilJob.getInvalidPath(params), Seq.empty[String]))

    outputDF.map(out => SaveParameters(out._1, out._2, out._3))

  }

  def castColumnsType(df: DataFrame) = df.select(
    df.columns.map {
      case time_taken @ "time_taken" => df(time_taken).cast(IntegerType).as(time_taken)
      case sc_status @ "sc_status" => df(sc_status).cast(IntegerType).as(sc_status)
      case cs_uri_port @ "cs_uri_port" => df(cs_uri_port).cast(IntegerType).as(cs_uri_port)
      case sc_bytes @ "sc_bytes" => df(sc_bytes).cast(LongType).as(sc_bytes)
      case cs_bytes @ "cs_bytes" => df(cs_bytes).cast(LongType).as(cs_bytes)
      case other         => df(other)
    }: _*
  )

  override def save(p: SaveParameters*) {
    p.foreach { out =>
      if (!out.df.head(1).isEmpty)
        out.df.write
          .partitionBy(out.partitions: _*)
          .options(out.params.outOptions)
          .format(out.params.outFormat)
          .mode(out.params.saveMode)
          .save(out.params.outPath)
    }
  }

}
