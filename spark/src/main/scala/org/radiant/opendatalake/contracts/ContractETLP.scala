package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import org.radiant.opendatalake.wap.WapETLP


abstract class ContractETLP(rc: RuntimeETLContext,
                            sourceDatasetId: String,
                            tablePrefix: String,
                            major: Int,
                            database: Option[String] = None) extends WapETLP(rc) {

  override lazy val mainDestination: DatasetConf =
    ContractDestination.forMajor(conf.getDataset(sourceDatasetId), tablePrefix, major, database)
}
