package databases

import io.github.cdimascio.dotenv.Dotenv

import java.util.Properties

object Postgres {
  private val dotenv = Dotenv.load()

  Class.forName(dotenv.get("POSTGRES_DRIVER"))

  val postgresUrl: String = dotenv.get("POSTGRES_CONNECTION_URL")
  val postgresDb: String = dotenv.get("POSTGRES_DATABASE")
  val postgresSchema: String = dotenv.get("POSTGRES_SCHEMA")
  private val username = dotenv.get("POSTGRES_USER")
  private val password = dotenv.get("POSTGRES_PASSWORD")

  val connectionProperties = new Properties()
  connectionProperties.put("user", username)
  connectionProperties.put("password", password)
}
