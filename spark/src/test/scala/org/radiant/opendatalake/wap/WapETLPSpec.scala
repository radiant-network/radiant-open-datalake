package org.radiant.opendatalake.wap

import bio.ferlab.datalake.commons.config.{DatalakeConf, SimpleConfiguration, StorageConf}
import bio.ferlab.datalake.commons.file.FileSystemType.S3
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class WapETLPSpec extends AnyFlatSpec with Matchers {

  private val conf = SimpleConfiguration(DatalakeConf(
    storages = List(
      StorageConf("iceberg_storage", "s3a://baked/iceberg/opendatalake_prd", S3),
      StorageConf("raw_storage", "s3a://baked/raw/landing", S3)
    ),
    sources = List(),
    sparkconf = Map()
  ))

  private def rootOf(c: SimpleConfiguration, id: String): String =
    c.storages.find(_.id == id).map(_.path).getOrElse(fail(s"no storage '$id'"))

  "withStorageRoot" should "replace only the targeted storage root" in {
    val out = WapETLP.withStorageRoot(conf, "iceberg_storage", "s3a://override/iceberg/opendatalake_qa")

    rootOf(out, "iceberg_storage") shouldBe "s3a://override/iceberg/opendatalake_qa"
    rootOf(out, "raw_storage") shouldBe "s3a://baked/raw/landing"
  }
}
