package etl

import common.Spark
import common.Spark.removeCollectionName
import constants.Common.{WHALE_THRESHOLD, valas}
import constants.Time.{endTimestamp, startTimestamp}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.WriteToFile.writeDataFrameToFile


object WalletEnricher extends BaseEnricher {
  private val walletsCollection = "token_wallets"

  private val tokenInfoDf = enrichTokenInfoDf()
  private val circulatingSupply = tokenInfoDf.select("circulatingSupply").first().getDouble(0)
  private val removePrefix = udf(removeCollectionName _)

  private def calculateIsWhale(df: DataFrame): DataFrame = {
    val isWhaleUDF = udf((balance: Double) => balance >= circulatingSupply * WHALE_THRESHOLD)
    val enrichedDf = df.withColumn("isWhale", isWhaleUDF(col("balance")))

    // Generate all timestamps
    val timestampsDf = df.select("timestamp").distinct()
    val allAddresses = df.select("address").distinct()
    val allAddressTimestamps = allAddresses.crossJoin(timestampsDf)

    // Join with enrichedDf
    val joined = allAddressTimestamps.join(
      enrichedDf,
      Seq("address", "timestamp"),
      "left_outer"
    )

    // Fill missing balance and isWhale values
    val filledDf = joined.withColumn("balance", coalesce(col("balance"), lit(0.0)))
      .withColumn("isWhale", coalesce(col("isWhale"), lit(false)))

    // Recreate isWhaleAndBalanceArray
    val groupedDf = filledDf
      .groupBy("address")
      .agg(
        map_from_entries(
          collect_list(
            struct(
              col("timestamp").as("key"),
              struct(
                col("isWhale"),
                col("balance")
              ).as("value")
            )
          )
        ).as("isWhaleAndBalanceArray")
      )

    groupedDf
  }




  //  private def calculateIsWhale(df: DataFrame): DataFrame = {
//    val isWhaleUDF = udf((balance: Double) => balance >= circulatingSupply * WHALE_THRESHOLD)
//    val enrichedDf = df.withColumn("isWhale", isWhaleUDF(col("balance")))
//    val groupedDf = enrichedDf
//      .groupBy("address")
//      .agg(
//        map_from_entries(
//          collect_list(
//            struct(
//              col("timestamp").as("key"),
//              struct(
//                col("isWhale"),
//                col("balance")
//              ).as("value")
//            )
//          )
//        ).as("isWhaleAndBalanceArray")
//      )
//    groupedDf
//  }

  override def fetchAndEnrichData(): Unit = {
    val selectedTransfersDf = getDataFromTimeBlock(
      startTimestamp = startTimestamp,
      endTimestamp = endTimestamp
    )


    val walletBalance = calculateBalance(selectedTransfersDf)
//    walletBalance.show()
//    walletBalance.filter(col("address") === "wallets/0x87f19351235aa47aba6200809d5a6f088d4dc3f0").show()
    val finalDf = calculateIsWhale(walletBalance)

    val finalGroupedDf = finalDf
      .withColumn("address", removePrefix(col("address")))
      .withColumn("_key", concat(lit(s"${valas}_"), col("address")))
      .withColumnRenamed("isWhaleAndBalanceArray", "balanceChangeLogs")


     Spark.saveDf(df = finalGroupedDf, collectionName = walletsCollection)

  }

  fetchAndEnrichData()
}
