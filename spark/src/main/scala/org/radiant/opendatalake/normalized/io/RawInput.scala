package org.radiant.opendatalake.normalized.io

import bio.ferlab.datalake.commons.config.{Configuration, DatalakeConf, DatasetConf, SimpleConfiguration}
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

object RawInput {

  /**
  	Read a raw dataset for a specific source version, injecting the raw storage root at runtime.
    */
  def readVersioned(datasetId: String, version: String, rawStorage: String)(
      implicit conf: Configuration,
      spark: SparkSession
  ): DataFrame = {
    val (dataset, overridden) = resolve(datasetId, version, rawStorage)
    dataset.read(overridden, spark)
  }

  /**
    Same source resolution as [[readVersioned]], but reads with an explicit schema instead of letting the
    reader infer one.

    Schema inference costs a full extra pass over the input, which is why this exists: a reader such as
    spark-xml would otherwise parse the whole release once to derive a schema and again to produce rows.
    It also pins the columns the job depends on, so a new element appearing upstream cannot change the
    shape of the extracted DataFrame.
    */
  def readVersionedWithSchema(datasetId: String, version: String, rawStorage: String, schema: StructType)(
      implicit conf: Configuration,
      spark: SparkSession
  ): DataFrame = {
    val (dataset, overridden) = resolve(datasetId, version, rawStorage)
    spark.read
      .format(dataset.format.sparkFormat)
      .schema(schema)
      .options(dataset.readoptions)
      .load(dataset.location(overridden))
  }

  /** The versioned dataset and a configuration whose raw storage root is the runtime-supplied one. */
  private def resolve(datasetId: String, version: String, rawStorage: String)(
      implicit conf: Configuration
  ): (DatasetConf, SimpleConfiguration) = {
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
    (dataset.replacePath("{{VERSION}}", version), overridden)
  }
}
