package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.RuntimeETLContext

case class NormalizerArgs(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, warehouse: Option[String] = None)
