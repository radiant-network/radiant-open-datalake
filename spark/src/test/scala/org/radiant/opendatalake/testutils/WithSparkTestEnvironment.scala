package org.radiant.opendatalake.testutils


import bio.ferlab.datalake.commons.config.{ConfigurationLoader, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL
import org.apache.commons.io.FileUtils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.io.File


trait WithSparkTestEnvironment  {

  private val tmp = new File("tmp").getAbsolutePath
  deleteRecursively(new File(tmp)) // Clean up before tests

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

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }

}
