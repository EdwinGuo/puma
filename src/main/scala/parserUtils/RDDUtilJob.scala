package parserUtils

import JodaTime
import org.apache.spark.rdd.RDD
import java.io.File
import org.apache.spark.sql.{Column, Row, SparkSession}
import scala.reflect.runtime.universe._
import scala.util._

object RDDUtilJob extends UtilJob with Serializable {

  def transformRDD(df: RDD[String], methodName: String)(f: String => Seq[String]): RDD[(String, (Try[Seq[String]], String))] =
    df.filter(!_.trim.isEmpty)
      .map(row => (row, (Try { f(row) }, methodName)))

  def parseAndConvertTimeRDD(df: RDD[String])(f: String => Seq[String])(implicit sparkParams: SparkParameters) = {
    val timestampIndex = getTimestampIndexBySource(sparkParams.parser)
    transformRDD(df, RhinoConstants.ParserMethod)(f) map { data =>
      (data._2._1.map(seq => converteTime(seq(timestampIndex))),
        if (data._2._1.isFailure) data._2._2 else "convertToTargetTZ",
        data._1,
        data._2._1)
    } map { data =>
      (data._1.map(row => data._4.get :+ data._4.get(timestampIndex) :+ row :+ JodaTime.getEpoch(row)),
        data._3,
        data._2)
    }
  }

  def parseAndConvertTime(df: RDD[String])(f: String => Seq[String])(implicit sparkParams: SparkParameters) = {
    val timestampIndex = getTimestampIndexBySource(sparkParams.parser)
    transformRDD(df, "parser")(f) map { data =>
      (data._2._1.map(seq => converteTime(seq(timestampIndex))),
        if (data._2._1.isFailure) data._2._2 else "convertToTargetTZ",
        data._1,
        data._2._1)
    } map { data =>
      (data._1.map(row => data._4.get :+ row :+ JodaTime.getEpoch(row)),
        data._3,
        data._2)
    }
  }

  def getInvalidDF[A](df: RDD[(Try[A], String, String)])(implicit spark: SparkSession, sparkParams: SparkParameters)  = {
    import spark.implicits._

    df.filter(_._1.isFailure)
      .map { row =>
      val errMsg = row._1 match {
        case Failure(msg) => msg.toString
        case Success(_) => ""
      }
      (row._2, errMsg, row._3)
    }
      .toDF(RDDUtilJob.invalidDFColumns: _*)
  }

  def getValidRDD(df: RDD[(Try[Seq[Any]], String, String)]): RDD[Row] =
    df.filter(_._1.isSuccess)
      .map(row => Row.fromSeq( row._1.get ))

  def getTimestampIndexBySource(sourceName: ParserName) = sourceName match {
    case "Infobox" | "FireEye" | "Panlogs" => 4
    case "BlueCoat"  => 0
    case "Snare" => 7
    case _ => 4
  }

  def createDFWithPartition[D <: Product : TypeTag](rdd: RDD[Row], col: Column)(implicit spark: SparkSession, sparkParams: SparkParams) = {
    import spark.implicits._

    val schema = Seq[D]().toDF.schema
    val ds = spark.createDataFrame(rdd, schema)

    ds.withColumn("event_date", col)
  }
}
