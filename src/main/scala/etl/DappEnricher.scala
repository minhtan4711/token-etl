package etl

import common.Spark
import common.Spark.removeCollectionName
import utils.HandleDataframe.joinDataframes
import org.apache.spark.sql.functions.{array_union, col, collect_list, collect_set, concat, count, explode, expr, lit, map, map_from_entries, monotonically_increasing_id, struct, sum, udf}
import constants.Common.valas
import constants.ProviderUrl.PATH_TO_JSON_DAPP_FILE
import org.apache.spark.sql.DataFrame
import constants.Time.{AN_HOUR, endTimestamp, startTimestamp}

object DappEnricher extends BaseEnricher {
  private val dappsCollection = dotenv.get("ARANGODB_DAPPS")
  private var dappAddresses: Seq[String] = Seq.empty
  private val removePrefix = udf(removeCollectionName _)

  private def getDappInfoFromTransfers(startTimestamp: Long, endTimestamp: Long): DataFrame = {
    val transfersDf = getDataFromTimeBlock(startTimestamp, endTimestamp)
    val addressesDf = getDistinctAddressFromDf(transfersDf)
    val dappDf = spark.read.option("multiline", "true").json(PATH_TO_JSON_DAPP_FILE)

    val explodedDf = dappDf.withColumn("contract_addresses", explode(dappDf("contract_addresses")))

    val joinedDf = addressesDf
      .join(explodedDf, addressesDf("address") === explodedDf("contract_addresses"))
      .drop("contract_addresses")
      .withColumnRenamed("_id", "idCMC")

    val groupedDf = joinedDf.groupBy("idCMC", "name", "image")
      .agg(collect_list("address").as("address"))
      .withColumn("_key", concat(lit(s"${valas}_"), col("idCMC")))
      .withColumn("id", monotonically_increasing_id())


    // get dapp address(es)
    import spark.implicits._
    val addressDf = groupedDf.select("address").as[Array[String]].collect()
    dappAddresses = addressDf.toSeq.flatten

    val dappAddressesWithPrefix = dappAddresses.map(address => s"wallets/$address")

    val dappTransfer = transfersDf
      .filter(
        col("_from").isin(dappAddressesWithPrefix: _*)
          ||
        col("_to").isin(dappAddressesWithPrefix: _*)
      )

    val dappAddressRelated = transfersDf
      .filter(
        col("_from").isin(dappAddressesWithPrefix: _*)
          ||
        col("_to").isin(dappAddressesWithPrefix: _*)
      )
      .select("_from", "_to")
      .distinct()
      .flatMap(row => Array(row.getString(0), row.getString(1)))
      .distinct()

    val unrelatedTransfer = transfersDf
      .filter(
        !col("_from").isin(dappAddressRelated.collect(): _*)
          &&
        !col("_to").isin(dappAddressRelated.collect(): _*)
          &&
        !col("_from").isin(dappAddressesWithPrefix: _*)
          &&
        !col("_to").isin(dappAddressesWithPrefix: _*)
      )

    // get address balance with remove prefix
//    val addressBalanceDF = calculateBalance(transfersDf)
//    val removeAddressBalanceDF = addressBalanceDF
//      .withColumn("address", removePrefix(col("address")))
    
    val unrelatedTransferWithHour = unrelatedTransfer
      .withColumn("hour", (col("transact_at") / AN_HOUR).cast("Long"))

    val unrelatedAddressByHour = unrelatedTransferWithHour
      .withColumn("_from", removePrefix(col("_from")))
      .withColumn("_to", removePrefix(col("_to")))
      .groupBy("hour")
      .agg(
        collect_set("_from").as("from_addresses"),
        collect_set("_to").as("to_addresses")
      )
      .withColumn("address", array_union(col("from_addresses"), col("to_addresses")))
      .drop("from_addresses", "to_addresses")
      .withColumn("timestamp", (col("hour") * AN_HOUR).cast("Long"))
      .drop("hour")

//    val explodedUnrelatedAddressByTimestamp = unrelatedAddressByHour
//      .select(col("timestamp"), explode(col("address")).as("address"))

//    val balanceUnrelatedDf = explodedUnrelatedAddressByTimestamp
//      .join(removeAddressBalanceDF, Seq("timestamp", "address"))
//      .groupBy("timestamp")
//      .agg(collect_list(struct("address", "balance")).as("addressBalance"))

//    val totalBalanceDf = balanceUnrelatedDf
//      .withColumn("address", expr("transform(addressBalance, x -> x.address)"))
//      .withColumn("totalBalance", expr("aggregate(addressBalance, 0D, (acc, x) -> acc + x.balance)"))
//      .drop("addressBalance")

//    val mapUnrelatedAddress = totalBalanceDf
//      .withColumn(
//        "other",
//        struct(
//          col("timestamp").as("key"),
//          struct(
//            col("address"),
//            col("totalBalance")
//          ).as("value")
//        )
//      )
//      .select(collect_list("other").as("other"))
//      .select(map_from_entries(col("other")).as("other"))
//      .withColumn("id", monotonically_increasing_id())

    val mapUnrelatedAddress = unrelatedAddressByHour
      .withColumn(
        "other",
        struct(
          col("timestamp").as("key"),
          struct(
            col("address")
          ).as("value")
        )
      )
      .select(collect_list("other").as("other"))
      .select(map_from_entries(col("other")).as("other"))
      .withColumn("id", monotonically_increasing_id())

    val dappTransferByHour = dappTransfer
      .withColumn("hour", (col("transact_at") / AN_HOUR).cast("Long"))

    // dataframe for send transfers from dapp
    val sendTransfer = dappTransferByHour
      .withColumn("_to", removePrefix(col("_to")))
      .filter(col("_from").isin(dappAddressesWithPrefix: _*))
      .groupBy("hour")
      .agg(
        collect_set("_to").as("sendAddresses"),
        count("*").as("numberOfSendTransfer"),
        sum("value").as("value")
      )
      .withColumn("timestamp", (col("hour") * AN_HOUR).cast("Long"))
      .drop("hour")

    val mapSendTransfer = sendTransfer
      .withColumn(
        "send",
        struct(
          col("timestamp").as("key"),
          struct(
            col("sendAddresses"),
            col("numberOfSendTransfer"),
            col("value")
          ).as("value")
        )
      )
      .select(collect_list("send").as("sendList"))
      .select(map_from_entries(col("sendList")).as("send"))
      .withColumn("id", monotonically_increasing_id())

    val receiveTransfer = dappTransferByHour
      .withColumn("_from", removePrefix(col("_from")))
      .filter(col("_to").isin(dappAddressesWithPrefix: _*))
      .groupBy("hour")
      .agg(
        collect_set("_from").as("addressesReceive"),
        count("*").as("numberOfReceiveTransfer"),
        sum("value").as("value")
      )
      .withColumn("timestamp", (col("hour") * AN_HOUR).cast("Long"))
      .drop("hour")

    val mapReceiveTransfer = receiveTransfer
      .withColumn(
        "receive",
        struct(
          col("timestamp").as("key"),
          struct(
            col("addressesReceive"),
            col("numberOfReceiveTransfer"),
            col("value")
          ).as("value")
        )
      )
      .select(collect_list("receive").as("receiveList"))
      .select(map_from_entries(col("receiveList")).as("receive"))
      .withColumn("id", monotonically_increasing_id())

    val dappDfs: Seq[DataFrame] = Seq(
      groupedDf,
      mapSendTransfer,
      mapReceiveTransfer,
      mapUnrelatedAddress
    )

    val finalDappDf = joinDataframes(dappDfs)

    finalDappDf.select(
      "_key",
      "idCMC",
      "address",
      "name",
      "image",
      "send",
      "receive",
      "other"
    )
  }


  override def fetchAndEnrichData(): Unit = {
    val dappDf = getDappInfoFromTransfers(startTimestamp, endTimestamp)
    Spark.saveDf(
      df = dappDf,
      collectionName = dappsCollection
    )
  }
  fetchAndEnrichData()
}
