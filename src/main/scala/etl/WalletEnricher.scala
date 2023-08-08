package etl

import common.Spark
import common.Spark.removeCollectionName
import constants.Common.{VALAS, VALAS_WHALE_THRESHOLD, VENUS, VENUS_WHALE_THRESHOLD, CAKE, CAKE_WHALE_THRESHOLD}
import constants.Time.{endTimestamp, startTimestamp}
import common.Coingecko.getCirculatingSupply
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


object WalletEnricher extends BaseEnricher {
  private val walletsCollection = "test_token_wallets"

  val validContractAddress = List(
    VALAS -> VALAS_WHALE_THRESHOLD,
    VENUS -> VENUS_WHALE_THRESHOLD,
    CAKE -> CAKE_WHALE_THRESHOLD
  )

  private val removePrefix = udf(removeCollectionName _)

  private def calculateIsWhale(df: DataFrame, circulatingSupply: Double, threshold: Double): DataFrame = {
    val isWhaleUDF = udf((balance: Double) => balance >= circulatingSupply * threshold)
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

    val filledDf = joined.withColumn("balance", coalesce(col("balance"), lit(0.0)))
      .withColumn("isWhale", coalesce(col("isWhale"), lit(false)))

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

  override def fetchAndEnrichData(): Unit = {
    validContractAddress.foreach { case (contract_address, threshold) =>
      val circulatingSupply = getCirculatingSupply(contract_address = contract_address)

      val selectedTransfersDf = getDataFromTimeBlock(
        startTimestamp = startTimestamp,
        endTimestamp = endTimestamp,
        contract_address = contract_address
      )

      val walletBalance = calculateBalance(selectedTransfersDf)
      val finalDf = calculateIsWhale(walletBalance, circulatingSupply, threshold)

      val finalGroupedDf = finalDf
        .withColumn("address", removePrefix(col("address")))
        .withColumn("_key", concat(lit(s"${contract_address}_"), col("address")))
        .withColumnRenamed("isWhaleAndBalanceArray", "balanceChangeLogs")

      Spark.saveDf(df = finalGroupedDf, collectionName = walletsCollection)
    }
  }



  fetchAndEnrichData()
}
