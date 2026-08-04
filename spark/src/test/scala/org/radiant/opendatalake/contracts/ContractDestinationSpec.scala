package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.Format.ICEBERG
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, TableConf}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ContractDestinationSpec extends AnyFlatSpec with Matchers {

  private val family = DatasetConf(
    id = "normalized_clinvar",
    storageid = "iceberg_storage",
    path = "/normalized/clinvar",
    format = ICEBERG,
    loadtype = OverWrite,
    table = Some(TableConf("reference", "clinvar")),
    partitionby = List("chromosome"),
    repartition = Some(Coalesce())
  )

  "tableName" should "carry the MAJOR" in {
    ContractDestination.tableName("clinvar", 1) shouldBe "clinvar_v1"
    ContractDestination.tableName("clinvar", 2) shouldBe "clinvar_v2"
  }

  "forMajor" should "name the table after the prefix and the MAJOR" in {
    val destination = ContractDestination.forMajor(family, "clinvar", 2)

    destination.path shouldBe "/normalized/clinvar_v2"
    destination.table.map(_.name) shouldBe Some("clinvar_v2")
    destination.table.map(_.database) shouldBe Some("reference")
  }

  it should "take an arbitrary prefix, not the family's table name" in {
    val destination = ContractDestination.forMajor(family, "clinvar_open", 1)

    destination.table.map(_.name) shouldBe Some("clinvar_open_v1")
    destination.path shouldBe "/normalized/clinvar_open_v1"
  }

  it should "handle a single-segment family path" in {
    val destination = ContractDestination.forMajor(family.copy(path = "/clinvar"), "clinvar_open", 1)

    destination.path shouldBe "/clinvar_open_v1"
    destination.table.map(_.name) shouldBe Some("clinvar_open_v1")
  }

  it should "leave the rest of the dataset alone" in {
    val destination = ContractDestination.forMajor(family, "clinvar", 1)

    destination.storageid shouldBe family.storageid
    destination.format shouldBe family.format
    destination.loadtype shouldBe family.loadtype
    destination.partitionby shouldBe family.partitionby
    destination.repartition shouldBe family.repartition
    destination.writeoptions shouldBe family.writeoptions
  }

  it should "give each MAJOR of a source a distinct table" in {
    val tables = List(1, 2, 3).map(ContractDestination.forMajor(family, "clinvar", _).table.map(_.name))

    tables.distinct should have size 3
  }

  it should "reject a family that declares no table" in {
    val ex = the[IllegalArgumentException] thrownBy ContractDestination.forMajor(family.copy(table = None), "clinvar", 1)
    ex.getMessage should include("declares no table")
  }

  it should "overwrite the last path segment even when it is not the family's table name" in {
    val odd = family.copy(path = "/normalized/raw_clinvar")

    ContractDestination.forMajor(odd, "clinvar", 1).path shouldBe "/normalized/clinvar_v1"
  }
}
