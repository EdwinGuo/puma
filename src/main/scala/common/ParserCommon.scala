package commons

object ParserCommon {
  case class SparkParameters(parser: String, inFormat: String = "", outFormat: String = "", inPath: String = "", outPath: String = "", saveMode: String = "error", inOptions: Map[String, String] = Map.empty[String, String], outOptions: Map[String, String] = Map.empty[String, String])


}
