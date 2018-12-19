package parserUtils

import org.joda.time.format._
import org.joda.time._

object JodaTime extends Serializable {

  def convertToTargetTZ(time: String, format: String = "yyyy-MM-dd'T'HH:mm:ssZ", zone: String = "EST") = {
    val fmt = DateTimeFormat.forPattern(format)
    val jt = fmt.parseDateTime(time)
    val newTime =  zone match {
      case "EST" => jt.toDateTime(DateTimeZone.forID("America/New_York")) //forId doesn't support the short forms, exception beign only UTC
      case "UTC" => jt.toDateTime(DateTimeZone.UTC)
      case _ => throw new Exception("Not support yet")
    }

    newTime
  }

  def getEpoch (dateTime: DateTime) = dateTime.getMillis / 1000

  def getEpoch (dateTime: String) = DateTime.parse(dateTime).getMillis / 1000

}
