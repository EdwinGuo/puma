package parser

object ProxyParser extends (String => Seq[String]) with Serializable {

  case class ProxySchema(time: Option[String], //4  convert to orig_timestamp
    syslog_timestamp: Option[String],//3 + T + 5
    time_taken: Option[String],//6                      Int
    c_ip: Option[String], // 7
    cs_username: Option[String],//8
    cs_auth_group: Option[String],//9
    x_exception_id: Option[String], //10
    sc_filter_result: Option[String], //11
    cs_categories: Option[String],//12
    cs_referer: Option[String],//13
    sc_status: Option[String],//14                       Int
    s_action: Option[String],//15
    cs_method: Option[String],//16
    rs_content_type: Option[String],//17
    cs_uri_scheme: Option[String],//18
    cs_host: Option[String],//19
    cs_uri_port: Option[String],//20                     Int
    cs_uri_path: Option[String],//21
    cs_uri_query: Option[String],//22
    cs_uri_extension: Option[String],//23
    cs_user_agent: Option[String],//24
    s_ip: Option[String],//25
    sc_bytes: Option[String],//26                         Long
    cs_bytes: Option[String],//27                         Long
    x_virus_id: Option[String],//28
    x_bluecoat_application_name: Option[String],//29
    x_bluecoat_application_operation: Option[String],//30
    orig_timestamp: Option[String], //convert of time
    iso_timestamp: Option[String],
    epoch_timestamp: Option[Long])

  override def apply(data: String) : Seq[String] = {
    parseData(data)
  }

  def parseData(data: String, delimiter: String = " ") = {
    // seperate the data by its delimiter
    var s1 = data.split(delimiter).filter(!_.isEmpty)
    // To retrieve the index for the quote column
    val indexsRange = s1.zipWithIndex
      .filter(tup => {tup._1.startsWith("\"") && !tup._1.endsWith("\"")} || {!tup._1.startsWith("\"") && tup._1.endsWith("\"")})
      .map(_._2).grouped(2).toList.map(inx => (inx(0), inx(1)))
    val concatStr = indexsRange.map(idx => s1.slice(idx._1, idx._2 + 1).mkString(" ")) zip indexsRange

    var pos = 0
    var newStr = List[(String, (Int, Int))]()

    for ((p0, (p1, p2)) <- concatStr) {
      var newPosition = p1 - pos
      var distance = p2 - p1 + 1
      newStr = newStr :+ (p0, (newPosition, distance))
      pos += p2 - p1
    }

    newStr.map(idx => s1 = s1.patch(idx._2._1, Seq(idx._1), idx._2._2))
    val result = s1.head.split("--") ++ s1.tail

    if(result.size != 31) {
      throw new IllegalArgumentException("Columns != 31")
    }

    val r2 = result.take(6)

    r2(4) +: (r2(3) + "T" + r2(5)) +: result.drop(6)
  }
}
