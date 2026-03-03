package org.radiant.opendatalake.config

import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import org.radiant.opendatalake.PublicDatasets

import pureconfig.generic.auto._

object EtlConfiguration extends App {
  val datalake_storage_id = "radiantodl_datalake"

  val qa_database = "radiantodl_qa"
  val staging_database = "radiantodl_staging"
  val prd_database = "radiantodl"

  val qa_storage = List(
    StorageConf(datalake_storage_id, "s3a://radiantodl-qa-app-datalake", S3),
  )
  val staging_storage = List(
    StorageConf(datalake_storage_id, "s3a://radiantodl-staging-app-datalake", S3),
  )
  val prd_storage = List(
    StorageConf(datalake_storage_id, "s3a://radiantodl-prd-app-datalake", S3)
  )

  val spark_conf = Map(
    // TODO: remove or adjust these confs when converting to iceberg
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.databricks.delta.merge.repartitionBeforeWrite.enabled" -> "true",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "true",
    "spark.databricks.delta.vacuum.parallelDelete.enabled" -> "true",
    "spark.delta.merge.repartitionBeforeWrite" -> "true"
  )

  val prd_spark_conf = spark_conf ++ Map(
    "hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
  )

  val sources = PublicDatasets(alias=datalake_storage_id, tableDatabase=Some("radiantodl"), viewDatabase=None).sources

  val qa_conf = SimpleConfiguration(DatalakeConf(
    storages = qa_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(qa_database, t.name)))),
    sparkconf = spark_conf
  ))

  val staging_conf = SimpleConfiguration(DatalakeConf(
    storages = staging_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(staging_database, t.name)))),
    sparkconf = spark_conf
  ))

  val prd_conf = SimpleConfiguration(DatalakeConf(
    storages = prd_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(prd_database, t.name)))),
    sparkconf = prd_spark_conf
  ))

  val test_conf = SimpleConfiguration(DatalakeConf(
    storages = List(),
    sources = sources,
    sparkconf = spark_conf
  ))

  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", qa_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/staging.conf", staging_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/prod.conf", prd_conf)
  ConfigurationWriter.writeTo("src/test/resources/config/test.conf", test_conf)
}
