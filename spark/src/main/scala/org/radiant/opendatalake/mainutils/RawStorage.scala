package org.radiant.opendatalake.mainutils

import mainargs.{ParserForClass, arg}

case class RawStorage(@arg(name = "raw-storage", doc = "s3a root for raw input, overrides the config storage root") value: String)

object RawStorage {
  implicit def configParser: ParserForClass[RawStorage] = ParserForClass[RawStorage]
}
