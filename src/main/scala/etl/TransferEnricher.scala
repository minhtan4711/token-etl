package etl

import common.Spark
import common.Spark.{addCollectionNameToAddress, createKeyForTransfersCollection}
import common.Web3.getBlockTimestampByTransactionHash
import databases.Postgres.{connectionProperties, postgresSchema, postgresUrl}
import databases.Arango.transfersCollectionSchema
import org.apache.spark.sql.functions.{col, udf}

import java.text.SimpleDateFormat
import java.util.Date
import scala.concurrent.{Await, ExecutionContext, Future}
import ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration

object TransferEnricher extends BaseEnricher {
  private val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
  private val startTime = System.currentTimeMillis()

  private val transfersCollection = dotenv.get("ARANGODB_TRANSFERS")
  private val transfersCollectionType = "edge"

  private val table = dotenv.get("POSTGRES_TRANSFER_EVENT_TABLE")
  private val source = s"$postgresSchema.$table"

  private val addPrefix = udf(addCollectionNameToAddress _)
  private val getTimestamp = udf(getBlockTimestampByTransactionHash _)
  private val createKey = udf(createKeyForTransfersCollection _)

  private val minBlockNumber = 16461079
  private val maxBlockNumber = 16535760
  private val blockSize = 1000

  private def processBlockRange(startBlockNumber: Int, endBlockNumber: Int): Future[Unit] = Future {
    val partitioningOptions = Map(
      "partitionColumn" -> "block_number",
      "lowerBound" -> s"$startBlockNumber",
      "upperBound" -> s"$endBlockNumber",
      "numPartitions" -> "8",
    )

    val query = s"(SELECT * FROM $source " +
      s"WHERE block_number " +
      s"BETWEEN $startBlockNumber AND $endBlockNumber ) " +
      s"as tmp"

    val currentBlockSizeDf = spark
      .read
      .options(partitioningOptions)
      .jdbc(postgresUrl, query, connectionProperties)

    val fixDf = currentBlockSizeDf
      .withColumn("_key",
        createKey(
          currentBlockSizeDf("log_index"),
          currentBlockSizeDf("block_number"),
          currentBlockSizeDf("from_address"),
          currentBlockSizeDf("to_address")
        )
      )
      .withColumn("log_index", col("log_index").cast("Integer"))
      .withColumn("block_number", col("block_number").cast("Integer"))
      .withColumnRenamed("from_address", "_from")
      .withColumnRenamed("to_address", "_to")

    val selectedDf = fixDf
      .withColumn("_from", addPrefix(fixDf("_from")))
      .withColumn("_to", addPrefix(fixDf("_to")))
      .select(
        "_key",
        "_from",
        "_to",
        "contract_address",
        "transaction_hash",
        "log_index",
        "block_number",
        "value",
      )

    logger.info("Getting timestamp...")
    val addTransactAtDf = selectedDf
      .withColumn("transact_at", getTimestamp(fixDf("transaction_hash")))
    logger.info("Getting timestamp done!")

    val arangoTransferDf = spark.createDataFrame(addTransactAtDf.rdd, transfersCollectionSchema)

    Spark.saveDf(
      df = arangoTransferDf,
      collectionName = "test_transfers",
      collectionType = transfersCollectionType)

    logger.info(s"Finished processing block number $startBlockNumber to $endBlockNumber")
  }

  private def processBlockRangeWithRetry(
    startBlockNumber: Int,
    endBlockNumber: Int,
    maxRetries: Int = 5
  ): Future[Unit] = {
    processBlockRange(startBlockNumber, endBlockNumber).recoverWith {
      case e: Exception =>
        if (maxRetries > 0) {
          logger.warn(s"Failed to process block range $startBlockNumber-$endBlockNumber due to ${e.getMessage}, retrying...")
          Thread.sleep(1000 * 10) // Wait 10 seconds before retrying
          processBlockRangeWithRetry(startBlockNumber, endBlockNumber, maxRetries - 1)
        } else {
          logger.error(s"Failed to process block range $startBlockNumber-$endBlockNumber after $maxRetries retries")
          Future.failed(e)
        }
    }
  }

  override def fetchAndEnrichData(): Unit = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Starting data fetch and enrichment at ${dateFormat.format(new Date())}")
    try {
      val batchSize = 5
      val delayBetweenBatches = 10

      val batchedBlockNumbers = (minBlockNumber to maxBlockNumber by blockSize).grouped(batchSize)

      batchedBlockNumbers.foreach { batch =>
        val futures = batch.map { startBlockNumber =>
          val endBlockNumber = startBlockNumber + blockSize - 1
          processBlockRangeWithRetry(startBlockNumber, endBlockNumber)
        }

        // Wait for each future in the batch to complete before proceeding
        futures.foreach { future =>
          try {
            Await.result(future, Duration.Inf)
          } catch {
            case e: Exception =>
              logger.error(s"A future failed with exception: ${e.getMessage}")
          }
          Thread.sleep(3000)
        }
        // Sleep for a certain duration between batches
        Thread.sleep(delayBetweenBatches * 1000)
        val endTime = System.currentTimeMillis()
        val elapsedTimeInSeconds = (endTime - startTime) / 1000
        logger.info(s"Data fetch and enrichment completed at ${dateFormat.format(new Date())}")
        logger.info(s"Total elapsed time: $elapsedTimeInSeconds seconds")
      }
    } catch {
      case e: Exception => println(e)
    }

  }
  fetchAndEnrichData()
  private val endTime = System.currentTimeMillis()
  private val elapsedTimeInSeconds = (endTime - startTime) / 1000
  logger.info(s"Entire program completed at ${dateFormat.format(new Date())}")
  logger.info(s"Total elapsed time for the entire program: $elapsedTimeInSeconds seconds")
}