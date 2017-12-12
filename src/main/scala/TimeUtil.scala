package Puma

object TimeUtil
{
  /**
    * return the hours in Int
    */
  def getTimeAsHour(timeStr: String, seperator: String = ":"): Int = {
    val s = timeStr.split(seperator)
    val hours = s(0).toInt
    hours
  }
}
