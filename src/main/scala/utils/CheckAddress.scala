package utils
import io.github.cdimascio.dotenv.Dotenv
import common.Spark.readFromArangoDB
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}


object CheckAddress extends App {
  protected val dotenv: Dotenv = Dotenv.load()
  private val dappsCollectionName = dotenv.get("ARANGODB_DAPPS")
  private val tokenWalletsCollectionName = dotenv.get("ARANGODB_TOKEN_WALLETS")


  def is_dapp_address(
                       token_address: String,
                       dapp_id: String,
                       wallet_address: String
                     ): Boolean = {
    val dappsCollection: DataFrame = readFromArangoDB(collectionName = dappsCollectionName)
    val dappKey = s"""${token_address}_${dapp_id}"""
    val dapp = dappsCollection.filter(col("_key") === dappKey)
    val isWalletAddressExist = dapp.filter(array_contains(col("address"), wallet_address)).count() > 0
    isWalletAddressExist
  }

  def is_whale_address(
                        token_address: String,
                        wallet_address: String,
                        start_timestamp: Int,
                        end_timestamp: Int
                      ): Boolean = {
    val tokenWalletsCollection = readFromArangoDB(collectionName = tokenWalletsCollectionName)
    val walletKey = s"${token_address}_${wallet_address}"

    val wallet = tokenWalletsCollection.filter(col("_key") === walletKey)

    val balanceChangeLogsSchema = MapType(StringType, StructType(Array(
      StructField("balance", LongType),
      StructField("isWhale", BooleanType)
    )))

    val balanceChangeLogsDF = wallet.select(
      from_json(
        to_json(col("balanceChangeLogs")), balanceChangeLogsSchema
      ).as("balanceChangeLogs"))

    val explodedDF = balanceChangeLogsDF.select(explode(col("balanceChangeLogs")).as(Seq("timestamp", "value")))

    val whaleDF = explodedDF.filter(
      col("timestamp").cast(LongType).between(start_timestamp, end_timestamp)
    ).select(
      col("timestamp"),
      col("value.balance").as("balance"),
      col("value.isWhale").as("isWhale")
    )

    val isWhale = whaleDF.filter(col("isWhale") === true).count() > 0
    isWhale
  }

//  is_dapp_address(
//    token_address = "0xb1ebdd56729940089ecc3ad0bbeeb12b6842ea6f",
//    dapp_id = "pancakeswap-amm",
//    wallet_address = "0x10ed43c718714eb63d5aa57b78b54704e256024e"
//  )

//  is_whale_address(
//    token_address = "0xb1ebdd56729940089ecc3ad0bbeeb12b6842ea6f",
//    wallet_address = "0x00000000008bef34003b59bed4c4c0f6f1543928",
//    start_timestamp = 1648494000,
//    end_timestamp = 1648494000 + 3599
//  )


}
