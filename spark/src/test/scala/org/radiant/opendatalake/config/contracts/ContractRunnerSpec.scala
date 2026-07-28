package org.radiant.opendatalake.config.contracts

import bio.ferlab.datalake.commons.config.Format.ICEBERG
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{DatasetConf, TableConf}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ContractRunnerSpec extends AnyFlatSpec with Matchers with OptionValues {

  private def iceberg(id: String, table: Option[String]): DatasetConf =
    DatasetConf(id, "iceberg_storage", s"/$id", ICEBERG, OverWrite, table = table.map(TableConf("reference", _)))

  private val twoMajors = Contracts.parse(
    """sources:
      |  clinvar:
      |    contracts:
      |      - lineage: "1.4"
      |        table: "clinvar"
      |        normalizer: "org.radiant.opendatalake.normalized.Clinvar"
      |        release_notes: "doc/release-notes/clinvar/v1.md"
      |      - lineage: "2.0"
      |        table: "clinvar_v2"
      |        normalizer: "org.radiant.opendatalake.normalized.DBSNP"
      |        release_notes: "doc/release-notes/clinvar/v2.md"
      |""".stripMargin
  )

  "plan" should "return every declared contract of the source, in file order" in {
    // Second row deliberately points at another registered normalizer: plan only cares that the
    // source fans out to all its MAJORs, in declaration order.
    ContractRunner.plan("clinvar", twoMajors).map(_.lineage) shouldBe List("1.4", "2.0")
  }

  it should "reject a source with no declared contract, listing what is declared" in {
    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("cosmic", twoMajors)
    ex.getMessage should include("No contract declared for source 'cosmic'")
    ex.getMessage should include("clinvar")
  }

  it should "reject two rows declaring the same MAJOR" in {
    val contracts = Contracts.parse(
      """sources:
        |  clinvar:
        |    contracts:
        |      - lineage: "1.0"
        |        table: "clinvar"
        |        normalizer: "org.radiant.opendatalake.normalized.Clinvar"
        |        release_notes: "v1.md"
        |      - lineage: "1.3"
        |        table: "clinvar"
        |        normalizer: "org.radiant.opendatalake.normalized.Clinvar"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("clinvar", contracts)
    ex.getMessage should include("same MAJOR more than once")
    ex.getMessage should include("MAJOR 1: 1.0, 1.3")
  }

  it should "reject a normalizer absent from the registry" in {
    val contracts = Contracts.parse(
      """sources:
        |  clinvar:
        |    contracts:
        |      - lineage: "1.0"
        |        table: "clinvar"
        |        normalizer: "org.radiant.opendatalake.normalized.NotRegistered"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("clinvar", contracts)
    ex.getMessage should include("absent from ContractRegistry")
    ex.getMessage should include("1.0 -> org.radiant.opendatalake.normalized.NotRegistered")
  }

  "destinationMismatch" should "accept a job writing to the table its contract declares" in {
    val contract = Contract("1.0", "clinvar", "org.radiant.opendatalake.normalized.Clinvar", "v1.md")
    ContractRunner.destinationMismatch(contract, iceberg("normalized_clinvar", Some("clinvar"))) shouldBe None
  }

  it should "report a job writing to a different table than declared" in {
    val contract = Contract("2.0", "clinvar_v2", "org.radiant.opendatalake.normalized.Clinvar", "v2.md")
    val mismatch = ContractRunner.destinationMismatch(contract, iceberg("normalized_clinvar", Some("clinvar")))

    mismatch.value should include("declares table 'clinvar_v2'")
    mismatch.value should include("writes to 'clinvar'")
  }

  it should "report a destination with no table at all" in {
    val contract = Contract("1.0", "clinvar", "org.radiant.opendatalake.normalized.Clinvar", "v1.md")
    val mismatch = ContractRunner.destinationMismatch(contract, iceberg("normalized_clinvar", None))

    mismatch.value should include("has no table in its destination")
  }

  "destinationMismatches" should "collect one message per disagreeing contract and none for a clean fan-out" in {
    val ok = Contract("1.0", "clinvar", "org.radiant.opendatalake.normalized.Clinvar", "v1.md")
    val wrongTable = Contract("2.0", "clinvar_v2", "org.radiant.opendatalake.normalized.DBSNP", "v2.md")
    val noTable = Contract("3.0", "clinvar_v3", "org.radiant.opendatalake.normalized.DBSNP", "v3.md")

    ContractRunner.destinationMismatches(
      List(ok -> iceberg("normalized_clinvar", Some("clinvar")))
    ) shouldBe empty

    val mismatches = ContractRunner.destinationMismatches(
      List(
        ok -> iceberg("normalized_clinvar", Some("clinvar")),
        wrongTable -> iceberg("normalized_dbsnp", Some("dbsnp")),
        noTable -> iceberg("normalized_dbsnp", None)
      )
    )

    mismatches should have size 2
    mismatches.head should include("declares table 'clinvar_v2'")
    mismatches(1) should include("has no table in its destination")
  }

  "plan" should "validate against an injected factory lookup rather than the real registry" in {
    val contracts = Contracts.parse(
      """sources:
        |  fake:
        |    contracts:
        |      - lineage: "1.0"
        |        table: "fake"
        |        normalizer: "com.example.NotInTheRegistry"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    ContractRegistry.factory("com.example.NotInTheRegistry") shouldBe None
    an[IllegalArgumentException] should be thrownBy ContractRunner.plan("fake", contracts)

    val accepting: ContractRunner.FactoryLookup = _ => Some(_ => fail("plan must not build jobs"))
    ContractRunner.plan("fake", contracts, accepting).map(_.lineage) shouldBe List("1.0")
  }
}
