package org.radiant.opendatalake.config.contracts

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, PropertyNamingStrategies}
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

case class Contract(lineage: String, table: String, releaseNotes: String) {

  private val parts: Array[String] = lineage.split('.')
  require(
    parts.length == 2 && parts.forall(p => p.nonEmpty && p.forall(_.isDigit)), s"Invalid lineage '$lineage', expected '{MAJOR}.{MINOR}' with numeric parts (e.g. '1.0')"
  )

  val major: Int = parts(0).toInt
  val minor: Int = parts(1).toInt
}

case class SourceContracts(contracts: List[Contract])

/*
  Jackson's nulls are normalized in `declaredSources`, once, so the rest of the class sees clean data:
  a yaml key present but empty deserializes to *null* and jackson-module-scala does not substitute an
  empty Map/List.
*/
case class Contracts(sources: Map[String, SourceContracts]) {

  private lazy val declaredSources: Map[String, SourceContracts] =
    Option(sources).getOrElse(Map.empty).map { case (name, declared) =>
      name -> SourceContracts(Option(declared).flatMap(d => Option(d.contracts)).getOrElse(Nil))
    }

  def forSource(source: String): List[Contract] = declaredSources.get(source).map(_.contracts).getOrElse(Nil)

  def sourceNames: Set[String] = declaredSources.keySet
}


object Contracts {

  val DefaultResource: String = "/contracts.yml"

  private lazy val mapper: ObjectMapper = {
    val m = new ObjectMapper(new YAMLFactory())
    m.registerModule(DefaultScalaModule)
    m.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
    m.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
    m
  }

  def load(resource: String = DefaultResource): Contracts = {
    val stream = Option(getClass.getResourceAsStream(resource))
      .getOrElse(throw new IllegalArgumentException(s"Contracts resource not found on classpath: $resource"))
    try mapper.readValue(stream, classOf[Contracts])
    finally stream.close()
  }

  // For test purposes only
  def parse(yaml: String): Contracts = mapper.readValue(yaml, classOf[Contracts])
}
