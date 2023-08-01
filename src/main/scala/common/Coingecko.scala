package common

import scalaj.http.Http

object Coingecko extends App {
  private val id = "binance-smart-chain"

  def getTokenIdFromAddress(contract_address: String): String = {
    val response = Http(s"https://api.coingecko.com/api/v3/coins/$id/contract/$contract_address").asString
    val jsonVal = ujson.read(response.body)
    val idCGK = jsonVal("id").str
    idCGK
  }

  def getNameFromAddress(contract_address: String): String = {
    val response = Http(s"https://api.coingecko.com/api/v3/coins/$id/contract/$contract_address").asString
    val jsonVal = ujson.read(response.body)
    val name = jsonVal("name").str
    name
  }

  def getSymbolFromAddress(contract_address: String): String = {
    val response = Http(s"https://api.coingecko.com/api/v3/coins/$id/contract/$contract_address").asString
    val jsonVal = ujson.read(response.body)
    val symbol = jsonVal("symbol").str
    symbol
  }

  def getLogoFromAddress(contract_address: String): String = {
    val response = Http(s"https://api.coingecko.com/api/v3/coins/$id/contract/$contract_address").asString
    val jsonVal = ujson.read(response.body)
    val logo = jsonVal("image").obj("small").str
    logo
  }

  def getTotalSupply(contract_address: String): Int = {
    val response = Http(s"https://api.coingecko.com/api/v3/coins/$id/contract/$contract_address").asString
    val jsonVal = ujson.read(response.body)
    val totalSupply = jsonVal("market_data").obj("total_supply").num.toInt
    totalSupply
  }

  def getMaxSupply(contract_address: String): Int = {
    val response = Http(s"https://api.coingecko.com/api/v3/coins/$id/contract/$contract_address").asString
    val jsonVal = ujson.read(response.body)
    val maxSupply = jsonVal("market_data").obj("max_supply").num.toInt
    maxSupply
  }

  def getCirculatingSupply(contract_address: String): Double = {
    val response = Http(s"https://api.coingecko.com/api/v3/coins/$id/contract/$contract_address").asString
    val jsonVal = ujson.read(response.body)
    val circulatingSupply = jsonVal("market_data").obj("circulating_supply").num
    circulatingSupply
  }
}
