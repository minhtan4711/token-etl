package etl

import common.Spark
import common.Spark.removeCollectionName
import utils.HandleDataframe.joinDataframes
import org.apache.spark.sql.functions.{array_union, col, collect_list, collect_set, concat, count, explode, expr, lit, map, map_from_entries, monotonically_increasing_id, struct, sum, udf}
import constants.ProviderUrl.PATH_TO_JSON_DAPP_FILE
import org.apache.spark.sql.DataFrame
import constants.Time.{AN_HOUR, endTimestamp, startTimestamp}
import scala.collection.mutable.ListBuffer

object DappEnricher extends BaseEnricher {
  private val dappsCollection = dotenv.get("ARANGODB_DAPPS")
  private var dappAddresses: Seq[String] = Seq.empty
  private val removePrefix = udf(removeCollectionName _)

  private def getDappInfoFromTransfers(startTimestamp: Long, endTimestamp: Long): DataFrame = {
    val validContractAddress = List(
      "0x0e09fabb73bd3ade0a17ecc321fd13a19e81ce82",
      "0xb1ebdd56729940089ecc3ad0bbeeb12b6842ea6f",
      "0xcf6bb5389c92bdda8a3747ddb454cb7a64626c63"
    )

    val allResults = ListBuffer[DataFrame]()

    validContractAddress.foreach{ contract_address =>
      val transfersDf = getDataFromTimeBlock(contract_address, startTimestamp, endTimestamp)
      val addressesDf = getDistinctAddressFromDf(transfersDf)
      val dappDf = spark.read.option("multiline", "true").json(PATH_TO_JSON_DAPP_FILE)

      val explodedDf = dappDf.withColumn("contract_addresses", explode(dappDf("contract_addresses")))

      val joinedDf = addressesDf
        .join(explodedDf, addressesDf("address") === explodedDf("contract_addresses"))
        .drop("contract_addresses")
        .withColumnRenamed("_id", "idCMC")

      val groupedDf = joinedDf.groupBy("idCMC", "name", "image")
        .agg(collect_list("address").as("address"))
        .withColumn("_key", concat(lit(s"${contract_address}_"), col("idCMC")))

      allResults += groupedDf
    }

    val finalDf = allResults.reduce(_ union _)

    val defaultImage = "path_to_default_image.jpg"
    val updatedFinalDf = finalDf.na.fill(Map("image" -> defaultImage))

    updatedFinalDf
  }


  override def fetchAndEnrichData(): Unit = {
    val dappDf = getDappInfoFromTransfers(startTimestamp, endTimestamp)
    Spark.saveDf(
      df = dappDf,
      collectionName = "test_dapps"
    )
  }
  fetchAndEnrichData()
}
