package constants

object Time {
  val A_MINUTE = 60
  val MINUTES_5 = 300
  val MINUTES_15 = 900
  val AN_HOUR = 3600
  val A_DAY = 86400
  val DAYS_2: Int = 2 * A_DAY
  val DAYS_3: Int = 3 * A_DAY
  val DAYS_7: Int = 7 * A_DAY
  val DAYS_30: Int = 30 * A_DAY
  val DAYS_31: Int = 31 * A_DAY
  val DAYS_100: Int = 100 * A_DAY
  val A_YEAR: Int = 365 * A_DAY

  val startTimestamp: Int = 1648494000
  val endTimestamp: Int = startTimestamp + DAYS_100 // 1657136406
}
