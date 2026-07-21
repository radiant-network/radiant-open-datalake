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
    val dataset = rc.config.getDataset(datasetId)
    val overridden = SimpleConfiguration(
      rc.config.datalake.copy(storages = rc.config.storages.map { storage =>
        if (storage.id == dataset.storageid) storage.copy(path = rawStorage) else storage
      })
    )
    dataset
      .replacePath("{{VERSION}}", version)
      .read(overridden, spark)
  }
}
