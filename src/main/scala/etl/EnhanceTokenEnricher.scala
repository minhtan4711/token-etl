package etl

import common.Spark
import constants.Time.{startTimestamp, endTimestamp}
import org.apache.spark.sql.DataFrame
import utils.HandleDataframe.joinDataframes

object EnhanceTokenEnricher extends BaseEnricher {
  private val tokensCollection = dotenv.get("ARANGODB_TOKENS_COLLECTION")

  override def fetchAndEnrichData(): Unit = {
    val selectedTransfersDf = getDataFromTimeBlock(
      startTimestamp = startTimestamp,
      endTimestamp = endTimestamp
    )
    val tokenInfoDf = enrichTokenInfoDf()

    val circulatingSupply = tokenInfoDf.select("circulatingSupply").first().getDouble(0)

    val numberOfTransactionChangeLogsDf = getNumberOfTransactionChangeLogs(selectedTransfersDf)

    val tradingVolumeChangeLogsDf = getTradingVolumeChangeLogs(selectedTransfersDf)

    val numberOfAddressChangeLogsDf = getNumberOfAddressChangeLogs(selectedTransfersDf)

    val numberOfDappChangeLogsDf = getNumberOfDappChangeLogs(selectedTransfersDf)

    val numberOfHolderChangeLogsDf = getNumberOfHolderChangeLogs(selectedTransfersDf)

    val numberOfWhaleWalletChangeLogsDf = getNumberOfWhaleWalletChangeLogs(selectedTransfersDf, circulatingSupply)

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

    Spark.saveDf(
      df = tokensDf,
      collectionName = tokensCollection
    )
  }
  fetchAndEnrichData()
}
