package etl

import common.{Coingecko, Spark, Web3}
import constants.Common.{LOW_NUMBER_OF_TRANSFER, MEDIUM_NUMBER_OF_TRANSFER}
import io.github.cdimascio.dotenv.Dotenv
import com.typesafe.scalalogging.Logger
import common.Spark.removeCollectionName
import constants.Time.{AN_HOUR, A_DAY}
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions.{array, coalesce, col, collect_list, count, countDistinct, explode, expr, first, lag, lit, map, map_concat, map_from_entries, monotonically_increasing_id, struct, sum, udf, when}
import org.apache.spark.sql.expressions.Window
import databases.Arango.connection
import utils.WriteToFile.writeDataFrameToFile
import utils.CheckAddress.{is_whale_address, is_dapp_address}


abstract class BaseEnricher extends App {
  protected val web3 = Web3.getWeb3Instance
  protected val spark = Spark.spark
  protected val dotenv = Dotenv.load()
  protected val logger: Logger = Logger(this.getClass)
  private val dappsCollection = "test_dapps"
  private val removePrefix = udf(removeCollectionName _)
  private val isDappAddress = udf(is_dapp_address _)
  private val isWhaleAddress = udf(is_whale_address _)

  protected def getDataFromTimeBlock(contract_address: String, startTimestamp: Long, endTimestamp: Long): DataFrame = {
    val queryTransfers =
      s"""
         |FOR transfer IN test_transfers
         |  FILTER
         |  transfer.contract_address == "$contract_address"
         |  AND
         |  TO_NUMBER(transfer.transact_at) >= $startTimestamp
         |  AND
         |  TO_NUMBER(transfer.transact_at) <= $endTimestamp
         |  RETURN transfer
         |""".stripMargin

    val transfersDf = spark.read
      .format("com.arangodb.spark")
      .options(connection ++ Map(
        "query" -> queryTransfers,
      ))
      .load()

    transfersDf
      .select(
        "_key",
        "_from",
        "_to",
        "contract_address",
        "transaction_hash",
        "log_index",
        "block_number",
        "value",
        "transact_at"
      )
  }

  protected def enrichTokenInfoDf(df: DataFrame): DataFrame = {
    val invalidAddresses = List(
      "0xd4cb328a82bdf5f03eb737f37fa6b370aef3e888",
      "0xf7de7e8a6bd59ed41a4b5fe50278b3b7f31384df"
    )
    val filteredDF = df.filter(!col("contract_address").isin(invalidAddresses: _*))

    val enrichedDF = filteredDF
      .withColumn("_key", df("contract_address"))
      .withColumn("contract_address", df("contract_address"))
      .withColumn("id", monotonically_increasing_id())

    enrichedDF
  }

  protected def getDistinctAddressFromDf(df: DataFrame): DataFrame = {
    val fromDf = df.select("_from").withColumnRenamed("_from", "address")
    val toDf = df.select("_to").withColumnRenamed("_to", "address")

    val addressesDf = fromDf.union(toDf).distinct()
    val removePrefixAddressDf = addressesDf
      .withColumn("address", removePrefix(col("address")))

    removePrefixAddressDf
  }

  protected def calculateBalance(df: DataFrame): DataFrame = {
    val transfersWithHour: DataFrame = df
      .withColumn("hour", (col("transact_at") / AN_HOUR).cast("long"))
    //new
    val fromAddressHourlyTransfers = transfersWithHour
      .select(
        col("hour"),
        col("_from").as("address"),
        (functions.negate(col("value"))).as("value")
      )

    val toAddressHourlyTransfers = transfersWithHour
      .select(col("hour"), col("_to").as("address"), col("value"))

    // Combine both dataframes
    val combinedTransfers = fromAddressHourlyTransfers.union(toAddressHourlyTransfers)

    // Group by hour and address, summing the values
    val hourlyTransfers = combinedTransfers
      .groupBy("hour", "address")
      .agg(sum("value").as("hourly_balance"))

    val hourlyTransfersWithTimestamp = hourlyTransfers
      .withColumn("timestamp", (col("hour") * AN_HOUR).cast("long"))
      .drop("hour")

    val window = Window
      .partitionBy("address")
      .orderBy("timestamp")
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val finalWalletCumulativeBalance = hourlyTransfersWithTimestamp
      .withColumn(
        "balance",
        sum("hourly_balance")
          .over(window)
      )
      .drop("hourly_balance")

    val sortedData = finalWalletCumulativeBalance.orderBy("address", "timestamp")
    sortedData
  }

  private def calculateBalanceWithWindow(df: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("address").orderBy("timestamp")
    df.withColumn("prevBalance", lag("balance", 1).over(windowSpec))
  }

  private def processBalance(
     df: DataFrame,
     balanceThreshold: Double,
     numberOfName: String,
     changeLogName: String
  ): DataFrame = {
    val withHolder = df
      .withColumn(
        "isHolder",
        when(
          col("balance") > balanceThreshold
            or
            (
              col("prevBalance") > balanceThreshold
                and
                col("balance").isNull
              ),
          true
        )
          .otherwise(false)
      )

    val holderCount = withHolder
      .withColumn("isHolderInt", col("isHolder").cast("Integer"))
      .groupBy("timestamp")
      .agg(sum("isHolderInt").as(numberOfName))

    val numberOfHolderChangeLogs = holderCount
      .withColumn(
        changeLogName,
        struct(
          col("timestamp").cast("Long").as("key"),
          col(numberOfName).cast("Int").as("value")
        )
      )
      .select(collect_list(changeLogName).as(changeLogName))
      .select(map_from_entries(col(changeLogName)).as(changeLogName))

    val numberOfHolderChangeLogsWithIndex: DataFrame = numberOfHolderChangeLogs
      .withColumn("id", monotonically_increasing_id())

    numberOfHolderChangeLogsWithIndex
  }

  protected def getNumberOfHolderChangeLogs(df: DataFrame): DataFrame = {
    val calculateData = calculateBalance(df)
    val processedData = calculateBalanceWithWindow(calculateData)
    processBalance(
      processedData,
      0,
      "numberOfHolder",
      "numberOfHolderChangeLogs"
    )
  }

  protected def getNumberOfWhaleWalletChangeLogs(
                                                  df: DataFrame,
                                                  circulatingSupply: Double,
                                                  whaleThreshHold: Double
                                                ): DataFrame = {
    val calculateData = calculateBalance(df)
    val processedData = calculateBalanceWithWindow(calculateData)
    processBalance(
      processedData,
      circulatingSupply * whaleThreshHold,
      "numberOfWhaleWallet",
      "numberOfWhaleWalletChangeLogs")
  }

  protected def getAverageNumberOfTransactionPerDay(df: DataFrame): DataFrame = {
    val transfersPerDay: DataFrame = df
      .withColumn("date", (col("transact_at") / A_DAY).cast("Long"))
      .groupBy("date")
      .count()

    val transfersWithTimestamp: DataFrame = transfersPerDay
      .withColumn("date", col("date") * A_DAY)

    val groupByAverageNumberOfTransactionPerDay = transfersWithTimestamp
      .groupBy("date")
      .sum("count")
      .withColumnRenamed("sum(count)", "numberOfTransactionPerDay")

    val averageNumberOfTransactionPerDay = groupByAverageNumberOfTransactionPerDay
      .withColumnRenamed("date", "timestamp")
      .withColumn("numberOfTransactionPerDay", (col("numberOfTransactionPerDay") / 24).cast("Int"))

    val mapAverageNumberOfTransactionPerDay = averageNumberOfTransactionPerDay
      .withColumn(
        "averageNumberOfTransactionPerDay",
        struct(
          col("timestamp").cast("Long").as("key"),
          col("numberOfTransactionPerDay").cast("Int").as("value")
        )
      )
      .select(collect_list("averageNumberOfTransactionPerDay").as("averageNumberOfTransactionPerDay"))
      .select(map_from_entries(col("averageNumberOfTransactionPerDay")).as("averageNumberOfTransactionPerDay"))
      .withColumn("id", monotonically_increasing_id())
      .drop("timestamp")
      .drop("numberOfTransactionPerDay")

    mapAverageNumberOfTransactionPerDay
  }

  protected def getNumberOfTransactionChangeLogs(df: DataFrame): DataFrame = {
    val transactionsByHour: DataFrame = df
      .withColumn("hour", (col("transact_at") / AN_HOUR).cast("long"))
      .groupBy("hour")
      .count()

    val transactionsByTimestamp: DataFrame = transactionsByHour
      .withColumnRenamed("hour", "timestamp")
      .withColumnRenamed("count", "numberOfTransfer")

    val transactionsMap = transactionsByTimestamp
      .withColumn("timestamp", col("timestamp") * AN_HOUR)
      .withColumn(
        "numberOfTransferChangeLogs",
        struct(
          col("timestamp").as("key"),
          col("numberOfTransfer").as("value")
        )
      )
      .select(collect_list("numberOfTransferChangeLogs").as("numberOfTransferChangeLogs"))
      .select(map_from_entries(col("numberOfTransferChangeLogs")).as("numberOfTransferChangeLogs"))
      .drop("numberOfTransfer")
      .drop("timestamp")

    val transactionsWithIndex = transactionsMap.withColumn("id", monotonically_increasing_id())
    transactionsWithIndex
  }

  protected def getTradingVolumeChangeLogs(df: DataFrame): DataFrame = {
    val transfersWithHourTimestamp: DataFrame = df
      .withColumn("timestamp", (col("transact_at") / AN_HOUR).cast("long"))

    val tradingVolumeByHour: DataFrame = transfersWithHourTimestamp
      .groupBy("timestamp")
      .agg(functions.sum("value").alias("tradingVolume"))
      .withColumn("timestamp", col("timestamp") * AN_HOUR)

//    tradingVolumeByHour

    val tradingVolumeMap: DataFrame = tradingVolumeByHour
      .withColumn(
        "tradingVolumeChanges",
        struct(
          col("timestamp").as("key"),
          col("tradingVolume").as("value")
        )
//        map(col("timestamp").cast("Long"), col("tradingVolume").cast("Double"))
      )
      .select(collect_list("tradingVolumeChanges").as("tradingVolumeChanges"))
      .select(map_from_entries(col("tradingVolumeChanges")).as("tradingVolumeChanges"))
      .drop("timestamp")
      .drop("tradingVolume")

//    val aggregatedVolumeChanges: DataFrame = tradingVolumeMap
//      .agg(collect_list("tradingVolumeChanges").alias("tradingVolumeChangeLogs"))

    val volumeChangesWithIndex: DataFrame = tradingVolumeMap
      .withColumn("id", monotonically_increasing_id())
    volumeChangesWithIndex
  }

  protected def getNumberOfAddressChangeLogs(df: DataFrame): DataFrame = {
    val walletsByHour: DataFrame = df
      .withColumn("timestamp", (col("transact_at") / AN_HOUR).cast("Long"))
      .withColumn("addresses", array(col("_from"), col("_to")))
      .select(col("timestamp"), explode(col("addresses")).as("address"))
      .groupBy("timestamp")
      .agg(countDistinct("address").as("numberOfUniqueWallet"))

    val walletChangesMap: DataFrame = walletsByHour
      .withColumn("timestamp", col("timestamp") * AN_HOUR)
      .withColumn(
        "numberOfAddressChangeLogs",
        struct(
          col("timestamp").as("key"),
          col("numberOfUniqueWallet").as("value")
        )
      )
      .select(collect_list("numberOfAddressChangeLogs").as("numberOfAddressChangeLogs"))
      .select(map_from_entries(col("numberOfAddressChangeLogs")).as("numberOfAddressChangeLogs"))
      .drop("timestamp")
      .drop("numberOfUniqueWallet")

    val walletChangesWithIndex: DataFrame = walletChangesMap
      .withColumn("id", monotonically_increasing_id())

    walletChangesWithIndex
  }

  protected def getNumberOfDappChangeLogs(df: DataFrame): DataFrame = {
    val removePrefix = udf(removeCollectionName _)
    val dappsDf = Spark.readFromArangoDB(dappsCollection)

    val fromDf = df
      .withColumn("hour", (col("transact_at") / AN_HOUR).cast("long"))
      .select(col("hour"), col("_from").as("address"))
      .withColumn("address", removePrefix(col("address")))

    val toDf = df
      .withColumn("hour", (col("transact_at") / AN_HOUR).cast("long"))
      .select(col("hour"), col("_to").as("address"))
      .withColumn("address", removePrefix(col("address")))

    val combinedDf = fromDf.union(toDf).distinct()

    val groupedByHourDf = combinedDf.groupBy("hour")
      .agg(collect_list("address").as("addresses"))

    val explodedDf = groupedByHourDf
      .select(col("hour"), explode(col("addresses")).as("address_explode"))

    import spark.implicits._
    val dappsDfExploded = dappsDf.select($"*",explode(col("address")).as("address_dapp_explode"))

    val dappsTransfers = explodedDf
      .join(dappsDfExploded, explodedDf("address_explode") === dappsDfExploded("address_dapp_explode"), "inner")
      .select("hour", "address", "name", "image")
      .drop("address_explode")

    val withDappInfo = dappsTransfers
      .withColumn("info", map(col("name"), col("image")))
      .drop("name")
      .drop("image")

    val groupedByHourWithInfo = withDappInfo.groupBy("hour")
      .agg(
        collect_list("info").as("dappInfo"),
        count("address").as("count")
      )

    val groupedByHourWithInfoTimestamp = groupedByHourWithInfo
      .withColumnRenamed("hour", "timestamp")
      .withColumn("timestamp", col("timestamp") * AN_HOUR)


    val finalDf = groupedByHourWithInfoTimestamp
      .withColumn(
      "numberOfDappChangeLogs",
        struct(
          col("timestamp").cast("Long").as("key"),
          col("count").cast("Int").as("value")
        )
      )
      .select(collect_list("numberOfDappChangeLogs").as("numberOfDappChangeLogs"))
      .select(map_from_entries(col("numberOfDappChangeLogs")).as("numberOfDappChangeLogs"))
      .withColumn("id", monotonically_increasing_id())
      .drop("count")

    finalDf
  }

  protected def processNumberOfTransferPerWallet(df: DataFrame) = {
    val dfWithHour = df
      .withColumn("hour", (col("transact_at") / AN_HOUR).cast("Long"))
      .withColumn("addresses", array(col("_from"), col("_to")))
      .select(col("hour"), explode(col("addresses")).as("address"))

    val transferCountDF = dfWithHour
      .groupBy("hour", "address")
      .count()



    val transferCountWithTimestampDF = transferCountDF
      .withColumn("timestamp", (col("hour") * AN_HOUR).cast("Long"))
      .drop("hour")

    val numberOfTransferPerAddressInTimestamp = transferCountWithTimestampDF
      .withColumnRenamed("address", "address")
      .withColumn("address", removePrefix(col("address")))
      .withColumnRenamed("count", "numberOfTransfers")

    val transferCountWithClusterDF: DataFrame = numberOfTransferPerAddressInTimestamp
      .withColumn(
        "walletCluster",
        when(
          col("numberOfTransfers") < LOW_NUMBER_OF_TRANSFER, "LOW")
          .when(
            col("numberOfTransfers") >= LOW_NUMBER_OF_TRANSFER
              &&
              col("numberOfTransfers") < MEDIUM_NUMBER_OF_TRANSFER, "MEDIUM"
          )
          .otherwise("HIGH")
      )

    val clusteredAddressesByTimestampDF = transferCountWithClusterDF
      .groupBy("timestamp", "walletCluster")
      .agg(collect_list("address").as("address"))


    val pivotClusteredDF = clusteredAddressesByTimestampDF
      .groupBy("timestamp")
      .pivot("walletCluster")
      .agg(
        first("address").as("addresses")
      )

    val finalDf = pivotClusteredDF
      .withColumn(
        "walletClusterByNumberOfTransfer",
        struct(
          col("timestamp").as("key"),
          struct(
            struct(
              coalesce(col("LOW"), array().cast("array<string>")).as("addresses")
            ).as("LOW"),
            struct(
              coalesce(col("MEDIUM"), array().cast("array<string>")).as("addresses")
            ).as("MEDIUM"),
            struct(
              coalesce(col("HIGH"), array().cast("array<string>")).as("addresses")
            ).as("HIGH")
          ).as("value")
        )
      )
      .select(collect_list("walletClusterByNumberOfTransfer").as("walletClusterByNumberOfTransfer"))
      .select(map_from_entries(col("walletClusterByNumberOfTransfer")).as("walletClusterByNumberOfTransfer"))
      .withColumn("id", monotonically_increasing_id())

    finalDf.show()

    finalDf
  }



  def fetchAndEnrichData(): Unit

}
