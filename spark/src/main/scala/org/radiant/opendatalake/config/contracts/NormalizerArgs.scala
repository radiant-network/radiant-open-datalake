package org.radiant.opendatalake.config.contracts

import bio.ferlab.datalake.commons.config.RuntimeETLContext

case class NormalizerArgs(rc: RuntimeETLContext, version: String, rawStorage: String)
