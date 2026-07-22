package org.radiant.opendatalake.mainutils

import mainargs.{ParserForClass, arg}

case class Version(@arg(name = "version", doc = "Source version, substituted into the raw read path") value: String)

object Version {
  implicit def configParser: ParserForClass[Version] = ParserForClass[Version]
}
