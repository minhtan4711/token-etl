package common

import constants.ProviderUrl.{
  PROVIDER_URL,
  PROVIDER_URL_1,
  PROVIDER_URL_2,
  PROVIDER_URL_3
}
import org.web3j.protocol.Web3j
import org.web3j.protocol.http.HttpService
import org.web3j.protocol.core.DefaultBlockParameter
import org.web3j.protocol.core.methods.response.EthBlock

import java.math.BigInteger
import scala.collection.mutable
import scala.util.{Failure, Random, Success, Try}

import com.typesafe.scalalogging.Logger


object Web3 {
  private val logger: Logger = Logger(this.getClass)
  private val PROVIDER_URLS: List[String] = List(
    PROVIDER_URL,
    PROVIDER_URL_1,
    PROVIDER_URL_2,
    PROVIDER_URL_3
  )
  private val random = new Random()
  private val blockTimestampCache = new mutable.HashMap[String, String]()
   

  def getWeb3Instance: Web3j = {
    val providerUrl = PROVIDER_URLS(random.nextInt(PROVIDER_URLS.length))
    logger.info(s"Using provider url: $providerUrl")
    Web3j.build(new HttpService(providerUrl))
  }

  def getBlockTimestampByBlockNumber(blockNumber: Int): String = {
    val web3: Web3j = getWeb3Instance
    val blockParameter = DefaultBlockParameter.valueOf(BigInteger.valueOf(blockNumber.longValue()))

    Try {
      val blockInfo: EthBlock = web3.ethGetBlockByNumber(blockParameter, false).send()
      blockInfo.getBlock.getTimestamp.toString()
    } match {
      case Success(timestamp) => timestamp
      case Failure(e) =>
        println(s"Exception when getting block timestamp for block $blockNumber: ${e.getMessage}")
        "null"
    }
  }

  def getBlockTimestampByTransactionHash(transaction_hash: String): String = {
    var web3: Web3j = getWeb3Instance
    try {
      for (i <- 0 to 3) {
        val ethTransaction = web3.ethGetTransactionByHash(transaction_hash).send()
        if (ethTransaction.getTransaction.isPresent) {
          val transaction = ethTransaction.getTransaction.get()
          val blockNumber = transaction.getBlockNumber
          // log content inside blockTimestampCache
          blockTimestampCache.foreach {
            case (key, value) => logger.info(s"blockTimestampCache: $key -> $value")
          }
          return blockTimestampCache.getOrElseUpdate(blockNumber.toString, {
            val block = web3
              .ethGetBlockByNumber(DefaultBlockParameter.valueOf(blockNumber), false)
              .send()
            block.getBlock.getTimestamp.toString()
          })
        }
        if (i == 0) {
          // retry with another provider
          val providerUrl = PROVIDER_URLS(random.nextInt(PROVIDER_URLS.length))
          logger.info(s"Retrying with provider url: $providerUrl")
          web3 = Web3j.build(new HttpService(providerUrl))
        }
      }
      throw new RuntimeException(s"Transaction not found: $transaction_hash")
    } catch {
      case e: Exception =>
        logger.warn(
          s"Exception occurred when getting block timestamp for transaction $transaction_hash: ${e.getMessage}"
        )
        null
    }
  }

}
