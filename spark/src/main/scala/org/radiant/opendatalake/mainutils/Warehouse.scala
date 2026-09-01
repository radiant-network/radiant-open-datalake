package org.radiant.opendatalake.mainutils

import mainargs.{ParserForClass, arg}

case class Warehouse(@arg(name = "warehouse", doc = "s3a root the Iceberg tables are located at, overrides the config iceberg_storage root") value: String)

object Warehouse {
  implicit def configParser: ParserForClass[Warehouse] = ParserForClass[Warehouse]
}
