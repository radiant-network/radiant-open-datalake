package org.radiant.opendatalake.mainutils

import mainargs.{ParserForClass, arg}

case class Database(@arg(name = "database", doc = "Iceberg database (namespace) the tables publish to, overrides the config database") value: String)

object Database {
  implicit def configParser: ParserForClass[Database] = ParserForClass[Database]
}
