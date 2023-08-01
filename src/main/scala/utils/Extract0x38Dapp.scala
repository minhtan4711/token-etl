package utils

import play.api.libs.json.{JsArray, JsObject, Json}

import java.io.PrintWriter
import scala.io.Source

object Extract0x38Dapp extends App {
  private val source = Source.fromFile("/Users/tanminh/knowledge_graph.projects.json")
  private val jsonString = try source.mkString finally source.close()
  val json = Json.parse(jsonString).as[JsArray]
  private val processedJson = json.value.filter { jsValue =>
    val sourcesExistAndValid = (jsValue \ "sources").toOption match {
      case Some(sources) => !sources.as[Seq[String]].contains("nft")
      case None => false
    }
    val contractAddressesExist = (jsValue \ "contractAddresses").asOpt[JsObject].isDefined

    sourcesExistAndValid && contractAddressesExist
  }.map { jsValue =>
     val contractAddresses = (jsValue \ "contractAddresses").asOpt[JsObject].getOrElse(Json.obj())
     val filteredContractAddresses = contractAddresses.fields.filter { case (key, _) => key.startsWith("0x38") }
       .map { case (key, _) => key }
       .map { key => key.substring(5) }
       .filter { key => key.nonEmpty }


     val id = (jsValue \ "_id").as[String]
     val name = (jsValue \ "name").as[String]
     Json.obj(
      "_id" -> id,
      "name" -> name,
      "contract_addresses" -> filteredContractAddresses
     )
  }
  private val filteredJson = processedJson.filter { jsValue =>
   (jsValue \ "contract_addresses").as[Seq[String]].nonEmpty
  }
  private val resultJson = JsArray(filteredJson)
  new PrintWriter("/Users/tanminh/dapp_0x38.json") {
    write(resultJson.toString)
    close()
  }

}
