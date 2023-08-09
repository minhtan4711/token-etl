package etl

import common.Spark
import constants.Time.{startTimestamp, endTimestamp}
import common.Coingecko.getCirculatingSupply
import org.apache.spark.sql.DataFrame
import utils.HandleDataframe.joinDataframes
import scala.collection.mutable.ListBuffer
import constants.Common.{VALAS, VALAS_WHALE_THRESHOLD, VENUS, VENUS_WHALE_THRESHOLD, CAKE, CAKE_WHALE_THRESHOLD}

object EnhanceTokenEnricher extends BaseEnricher {
  private val tokensCollection = dotenv.get("ARANGODB_TOKENS_COLLECTION")

  override def fetchAndEnrichData(): Unit = {
    val validContractAddress = List(
      VALAS -> VALAS_WHALE_THRESHOLD,
      VENUS -> VENUS_WHALE_THRESHOLD,
      CAKE -> CAKE_WHALE_THRESHOLD
    )

    val allResults = ListBuffer[DataFrame]()

    validContractAddress.foreach { case (contract_address, threshold) =>
      val selectedTransfersDf = getDataFromTimeBlock(
        startTimestamp = startTimestamp,
        endTimestamp = endTimestamp,
        contract_address = contract_address
      )
      val tokenInfoDf = enrichTokenInfoDf(selectedTransfersDf)

      val circulatingSupply = getCirculatingSupply(contract_address = contract_address)

      val numberOfTransactionChangeLogsDf = getNumberOfTransactionChangeLogs(selectedTransfersDf)

      val tradingVolumeChangeLogsDf = getTradingVolumeChangeLogs(selectedTransfersDf)

      val numberOfAddressChangeLogsDf = getNumberOfAddressChangeLogs(selectedTransfersDf)

      val numberOfDappChangeLogsDf = getNumberOfDappChangeLogs(df = selectedTransfersDf, contract_address = contract_address)

      val numberOfHolderChangeLogsDf = getNumberOfHolderChangeLogs(selectedTransfersDf)

      val numberOfWhaleWalletChangeLogsDf = getNumberOfWhaleWalletChangeLogs(
        selectedTransfersDf,
        circulatingSupply,
        threshold
      )

      val averageNumberOfTransactionPerDayChangeLogs =
        getAverageNumberOfTransactionPerDay(selectedTransfersDf)

      val transferCount = processNumberOfTransferPerWallet(selectedTransfersDf)

      val dfs: Seq[DataFrame] = Seq(
        tokenInfoDf,
        numberOfTransactionChangeLogsDf,
        tradingVolumeChangeLogsDf,
        numberOfAddressChangeLogsDf,
        numberOfDappChangeLogsDf,
        numberOfHolderChangeLogsDf,
        numberOfWhaleWalletChangeLogsDf,
        averageNumberOfTransactionPerDayChangeLogs,
        transferCount
      )

      val tokensDf = joinDataframes(dfs)

      allResults += tokensDf

    }

    val finalResultDf = allResults.reduce(_ union _)
    val insertedDf = finalResultDf.drop(
      "_from",
      "_to",
      "contract_address",
      "transaction_hash",
      "log_index",
      "block_number",
      "value",
      "transact_at"
    )

    Spark.saveDf(
      df = insertedDf,
      collectionName = "test_tokens"
    )

  }
  fetchAndEnrichData()
}
