package org.radiant.opendatalake.config

import bio.ferlab.datalake.commons.config.{RuntimeETLContext, SimpleConfiguration}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.{DataFrame, SparkSession}

object RawInput {

  /**
  	Read a raw dataset for a specific source version, injecting the raw storage root at runtime.
    */
  def readVersioned(rc: RuntimeETLContext, datasetId: String, version: String, rawStorage: String)(
      implicit spark: SparkSession
  ): DataFrame = {
    val overridden = SimpleConfiguration(
      rc.config.datalake.copy(storages = rc.config.storages.map { storage =>
        if (storage.id == EtlConstants.RawStorageId) storage.copy(path = rawStorage) else storage
      })
    )
    rc.config
      .getDataset(datasetId)
      .replacePath(EtlConstants.VersionPlaceholder, version)
      .read(overridden, spark)
  }
}
