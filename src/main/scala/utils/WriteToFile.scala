package utils

import org.apache.spark.sql.DataFrame

object WriteToFile {
  def writeDataFrameToFile(df: DataFrame): Unit = {
    df.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("src/main/resources/output")
  }
}
