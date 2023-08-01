package utils

import constants.Time.DAYS_100

object GetTimestamp extends App {
  def getTimestamp100daysBefore(current_timestamp: String): String = {
    val currentTimestampAsLong = current_timestamp.toLong
    val timestamp100daysBefore = currentTimestampAsLong - DAYS_100
    return timestamp100daysBefore.toString
  }
  def getTimestamp100daysAfter(current_timestamp: String): String = {
    val currentTimestampAsLong = current_timestamp.toLong
    val timestamp100daysBefore = currentTimestampAsLong + DAYS_100
    return timestamp100daysBefore.toString
  }

}
