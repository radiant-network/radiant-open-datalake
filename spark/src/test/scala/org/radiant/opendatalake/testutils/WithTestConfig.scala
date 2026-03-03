package org.radiant.opendatalake.testutils

import bio.ferlab.datalake.commons.config.{ConfigurationLoader, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.LOCAL

trait WithTestConfig {
  val alias = "radiantodl_datalake"
  lazy val sc: SimpleConfiguration = ConfigurationLoader.loadFromResources[SimpleConfiguration]("config/test.conf")
  implicit lazy val conf: SimpleConfiguration =
    sc
      .copy(datalake = sc.datalake.copy(storages = List(StorageConf(alias, getClass.getClassLoader.getResource(".").getFile, LOCAL))))
}
