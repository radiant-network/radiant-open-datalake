package org.radiant.opendatalake.normalized.io

import bio.ferlab.datalake.commons.config.{Configuration, DatalakeConf, SimpleConfiguration}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.{DataFrame, SparkSession}

object RawInput {

  /**
  	Read a raw dataset for a specific source version, injecting the raw storage root at runtime.
    */
  def readVersioned(datasetId: String, version: String, rawStorage: String)(
      implicit conf: Configuration,
      spark: SparkSession
  ): DataFrame = {
    val dataset = conf.getDataset(datasetId)
    val overridden = SimpleConfiguration(
      DatalakeConf(
        storages = conf.storages.map { storage =>
          if (storage.id == dataset.storageid) storage.copy(path = rawStorage) else storage
        },
        sources = conf.sources,
        args = conf.args,
        sparkconf = conf.sparkconf
      )
    )
    dataset
      .replacePath("{{VERSION}}", version)
      .read(overridden, spark)
  }
}
