package databases

import org.apache.spark.sql.types.{StructField, _}
import io.github.cdimascio.dotenv.Dotenv

object Arango {
  private val dotenv: Dotenv = Dotenv.load()

  val connection: Map[String, String] = Map(
    "user" -> dotenv.get("ARANGODB_USERNAME"),
    "password" -> dotenv.get("ARANGODB_PASSWORD"),
    "endpoints" -> s"${dotenv.get("ARANGODB_HOST")}:${dotenv.get("ARANGODB_PORT")}",
    "database" -> dotenv.get("ARANGODB_DATABASE"),
  )

  val tokensCollectionSchema: StructType = StructType(
    List(
      StructField("_key", StringType, nullable = false),
      StructField("address", StringType, nullable = false),
      StructField("decimals", StringType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("symbol", StringType, nullable = false),
      StructField("logo", StringType, nullable = false)
    )
  )

  val transfersCollectionSchema: StructType = StructType(
    List(
      StructField("_key", StringType, nullable = false),
      StructField("_from", StringType, nullable = false),
      StructField("_to", StringType, nullable = false),
      StructField("contract_address", StringType, nullable = false),
      StructField("transaction_hash", StringType, nullable = false),
      StructField("log_index", IntegerType, nullable = false),
      StructField("block_number", IntegerType, nullable = false),
      StructField("value", DoubleType, nullable = false),
      StructField("transact_at", StringType, nullable = false)
    )
  )

  val walletsCollectionSchema: StructType = StructType(
    List(
      StructField("_key", StringType, nullable = false),
      StructField("address", StringType, nullable = false)
    )
  )

  val dappsCollectionSchema: StructType = StructType(
    List(
      StructField("_key", StringType, nullable = false),
      StructField("address", StringType, nullable = false),
      StructField("name", StringType, nullable = false)
    )
  )
}
