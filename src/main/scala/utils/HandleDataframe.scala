package utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}

object HandleDataframe {
  def joinDataframes(
    dataFrames: Seq[DataFrame],
    joinType: String = "inner",
    joinColumn: String = "id")
  : DataFrame = {
      val joinedDf = dataFrames.reduce((df1, df2) => df1.join(df2, joinColumn, joinType))
      joinedDf.drop(joinColumn)
  }
}
