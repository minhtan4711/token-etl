package common

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import io.github.cdimascio.dotenv.Dotenv
import databases.Arango.connection
import databases.Postgres.{connectionProperties, postgresSchema, postgresUrl}
import org.apache.spark.SparkContext


object Spark {
  private val dotenv = Dotenv.load()

  val spark: SparkSession = SparkSession.builder
    .appName("PostgresToArangoETL")
    .master("local[8]")
    .getOrCreate()

  // define spark context
  val sc: SparkContext = spark.sparkContext

  private val defaultSaveOptions: Map[String, String] = Map(
    "database" -> dotenv.get("ARANGODB_DATABASE"),
    "user" -> dotenv.get("ARANGODB_USERNAME"),
    "password" -> dotenv.get("ARANGODB_PASSWORD"),
    "endpoints" -> s"${dotenv.get("ARANGODB_HOST")}:${dotenv.get("ARANGODB_PORT")}",
    "keep.null" -> "true",
    "overwrite.mode" -> "update"
  )

  def addCollectionNameToAddress(address: String): String = {
    return s"wallets/$address"
  }

  def removeCollectionName(address: String): String = {
    val parts = address.split("/")
    if (parts.length > 1) {
      return parts(1)
    } else {
      return address
    }
  }

  def createKeyForTransfersCollection(
    log_index: Int,
    block_number: Int,
    from_address: String,
    to_address: String
  ): String = {
    val timestamp = System.currentTimeMillis().toString
    return s"${log_index.toString}" + "_" + s"${block_number.toString}" + "_" + s"$from_address" + "_" + s"$to_address" + "_" + s"$timestamp"
  }

  def readFromArangoDB(collectionName: String): DataFrame = {
    spark.read
      .format("com.arangodb.spark")
      .options(connection ++ Map(
        "table" -> collectionName
      ))
      .load()
  }

  def readFromPostgres(tableName: String): DataFrame = {
    val source = s"$postgresSchema.$tableName"
    spark.read
      .jdbc(postgresUrl, source, connectionProperties)
  }

  def saveDf
  (
    df: DataFrame,
    collectionName: String,
    collectionType: String = "document",
  ): Unit = {
    df
      .write
      .format("com.arangodb.spark")
      .mode(SaveMode.Append)
      .options(defaultSaveOptions ++ Map(
        "table" -> collectionName,
        "table.type" -> collectionType,
        "retry.minDelay" -> "5000",
        "retry.maxDelay" -> "8000",
      ))
      .save()
  }


}
