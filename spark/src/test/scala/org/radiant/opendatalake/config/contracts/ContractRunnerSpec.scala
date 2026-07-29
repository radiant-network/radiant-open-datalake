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
      |        release_notes: "doc/release-notes/clinvar/v1.md"
      |      - lineage: "2.0"
      |        table: "clinvar_v2"
      |        release_notes: "doc/release-notes/clinvar/v2.md"
      |""".stripMargin
  )

  /*
    Accepts anything, and fails loudly if a caller tries to build with it. Lets the plan specs use
    contracts that no real registry entry backs — plan is about selection, not registration.
  */
  private val acceptAll: ContractRunner.FactoryLookup = _ => Some(_ => fail("plan must not build jobs"))

  "plan" should "return every declared contract of the source, in file order" in {
    ContractRunner.plan("clinvar", twoMajors, acceptAll).map { case (c, _) => c.lineage } shouldBe List("1.4", "2.0")
  }

  it should "reject a source with no declared contract" in {
    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("cosmic", twoMajors)
    ex.getMessage should include("No contract declared for source 'cosmic'")
  }

  it should "reject two rows declaring the same MAJOR" in {
    val contracts = Contracts.parse(
      """sources:
        |  clinvar:
        |    contracts:
        |      - lineage: "1.0"
        |        table: "clinvar"
        |        release_notes: "v1.md"
        |      - lineage: "1.3"
        |        table: "clinvar"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("clinvar", contracts)
    ex.getMessage should include("same MAJOR more than once")
    ex.getMessage should include("'clinvar'")
    ex.getMessage should include("MAJOR 1: 1.0, 1.3") // the colliding lineages, not just the fact of a collision
  }

  it should "reject a table with no registry entry" in {
    val contracts = Contracts.parse(
      """sources:
        |  clinvar:
        |    contracts:
        |      - lineage: "1.0"
        |        table: "clinvar_not_registered"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("clinvar", contracts)
    ex.getMessage should include("No ContractRegistry entry for source 'clinvar'")
    ex.getMessage should include("(clinvar_not_registered, MAJOR 1)")
  }

  /*
    MAJOR is half the registry key, so a declared table whose registered normalizer belongs to another
    MAJOR is not a match. Without MAJOR in the key this row would resolve to the MAJOR 1 normalizer and
    silently publish it as MAJOR 2.
  */
  it should "reject a registered table declared under a MAJOR the registry does not know" in {
    val contracts = Contracts.parse(
      """sources:
        |  clinvar:
        |    contracts:
        |      - lineage: "2.0"
        |        table: "clinvar"
        |        release_notes: "v2.md"
        |""".stripMargin
    )

    ContractRegistry.factory(Contract("1.0", "clinvar", "v1.md")) should not be empty

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("clinvar", contracts)
    ex.getMessage should include("No ContractRegistry entry for source 'clinvar'")
    ex.getMessage should include("(clinvar, MAJOR 2)")
  }

  "destinationMismatch" should "accept a job writing to the table its contract declares" in {
    val contract = Contract("1.0", "clinvar", "v1.md")
    ContractRunner.destinationMismatch(contract, iceberg("normalized_clinvar", Some("clinvar"))) shouldBe None
  }

  it should "report a job writing to a different table than declared" in {
    val contract = Contract("2.0", "clinvar_v2", "v2.md")
    val mismatch = ContractRunner.destinationMismatch(contract, iceberg("normalized_clinvar", Some("clinvar")))

    mismatch.value should include("declares table 'clinvar_v2'")
    mismatch.value should include("writes to 'clinvar'")
  }

  it should "report a destination with no table at all" in {
    val contract = Contract("1.0", "clinvar", "v1.md")
    val mismatch = ContractRunner.destinationMismatch(contract, iceberg("normalized_clinvar", None))

    mismatch.value should include("writes to no table")
  }

  "plan" should "validate against an injected factory lookup rather than the real registry" in {
    val contracts = Contracts.parse(
      """sources:
        |  fake:
        |    contracts:
        |      - lineage: "1.0"
        |        table: "fake"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    ContractRegistry.factory(Contract("1.0", "fake", "v1.md")) shouldBe None
    an[IllegalArgumentException] should be thrownBy ContractRunner.plan("fake", contracts)

    ContractRunner.plan("fake", contracts, acceptAll).map { case (c, _) => c.lineage } shouldBe List("1.0")
  }
}
