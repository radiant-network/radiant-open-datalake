package org.radiant.opendatalake.testutils


import bio.ferlab.datalake.commons.config.{ConfigurationLoader, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.io.File
import java.nio.file.Files


trait WithSparkTestEnvironment extends BeforeAndAfterEach {
  this: AnyFlatSpec =>

  /*
  We use a fixed tmp folder for all tests to ensure consistency between the Spark warehouse path
  (spark.sql.catalog.opendatalake.warehouse) and the storage configuration (in the StorageConf list).
  Due to SparkSession's .getOrCreate singleton behavior, the same Spark instance is reused across tests,
  so we cannot assign a different tmp folder per test.
  */
  private val tmp = new File("tmp").getAbsolutePath

  private val test_conf = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/test.conf")

  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config(test_conf.sparkconf)
    .config("spark.sql.catalog.opendatalake.type", "hadoop")
    .config("spark.sql.catalog.opendatalake.warehouse", s"file:$tmp/warehouse")
    .master("local")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  implicit lazy val conf: SimpleConfiguration = {
    test_conf.copy(datalake = test_conf.datalake.copy(storages = List(
      StorageConf("raw_storage", getClass.getClassLoader.getResource(".").getFile, LOCAL),
      StorageConf("iceberg_storage", s"$tmp/warehouse/reference", LOCAL)
    )))
  }

  // Clean up the tmp directory before and after each test to ensure a clean state
  override def beforeEach(): Unit = {
    deleteRecursively(new File(tmp))
    super.beforeEach()
  }

  override def afterEach(): Unit = {
    deleteRecursively(new File(tmp))
    super.afterEach()
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val files = file.listFiles()
      if (files != null) {
        files.foreach(deleteRecursively)
      }
    }
    if (file.exists()) {
      assert(file.delete(), s"Failed to delete file or directory: ${file.getAbsolutePath}")
    }
  }
}
