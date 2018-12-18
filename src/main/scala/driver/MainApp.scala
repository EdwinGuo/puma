package Driver

import parserUtils.Utils._
import org.apache.commons.lang.SystemUtils
import scala.reflect.runtime.universe
import commons.ParserCommon._

object MainApp {

  def main(args: Array[String]): Unit = {

    if (SystemUtils.IS_OS_WINDOWS && System.getProperty("hadoop.home.dir") == null && System.getenv("HADOOP_HOME") == null) {
      System.setProperty("hadoop.home.dir", "c:/winutils/")
    }

    val sparkParams = parseCmd().parse(args, SparkParameters())

    val className = sparkParams match {
      case Some(x) => Some(sparkParams.get.parser.toString)
      case _ => None
    }

    if (className isDefined) {
      try {
        invoke(className, "run", sparkParams)
      } catch {
        case _: ClassNotFoundException => println(className.get)
      }
    } else {
      throw new Exception("Class Not Found, please define class name!")
    }
  }

  def parseCmd() = new scopt.OptionParser[SparkParameters]("parser") {
    head("Job-parser", "x.y.z")

    opt[ParserName.Value]('p', "parser").required().valueName("<parser>").
      action((x, c) => c.copy(parser = x)).
      text(s"Parser name is required")

    opt[String]('i', "input-format").required().valueName("<input-format>").
      action((x, c) => c.copy(inFormat = x)).
      text("Input format is required.")

    opt[String]('o', "output-format").required().valueName("<output-format>").
      action((x, c) => c.copy(outFormat = x)).
      text("Output format is required.")

    opt[String]('s', "input-path").required().valueName("<input-path>").
      action((x, c) => c.copy(inPath = x)).
      text("Input path is required.")

    opt[String]('d', "output-path").required().valueName("<output-path>").
      action((x, c) => c.copy(outPath = x)).
      text("Output path is required.")

    opt[String]('m', "save-mode").optional().valueName("<save-mode>").
      action((x, c) => c.copy(saveMode = x)).
      text("Output save mode (error, append, overwrite, ignore).")

    opt[Map[String, String]]("input-options").optional().valueName("k1=v1,k2=v2...").action((x, c) =>
      c.copy(inOptions = x)).text("Spark read input options")

    opt[Map[String, String]]("output-options").optional().valueName("k1=v1,k2=v2...").action((x, c) =>
      c.copy(outOptions = x)).text("Spark write output options")
  }
}
