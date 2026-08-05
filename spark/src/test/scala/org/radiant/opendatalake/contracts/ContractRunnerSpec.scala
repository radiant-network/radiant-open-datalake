package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.Format.ICEBERG
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{DatasetConf, TableConf}
import org.radiant.opendatalake.config.{Contract, Contracts}
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class ContractRunnerSpec extends AnyFlatSpec with Matchers with OptionValues {

  private def iceberg(id: String, table: Option[String]): DatasetConf =
    DatasetConf(id, "iceberg_storage", s"/$id", ICEBERG, OverWrite, table = table.map(TableConf("reference", _)))

  private val twoMajors = Contracts.parse(
    """sources:
      |  clinvar:
      |    table_prefix: "clinvar"
      |    contracts:
      |      - lineage: "1.4"
      |        release_notes: "doc/release-notes/clinvar/v1.md"
      |      - lineage: "2.0"
      |        release_notes: "doc/release-notes/clinvar/v2.md"
      |""".stripMargin
  )

  private val acceptAll: ContractRunner.FactoryLookup = (_, _) => Some(_ => fail("plan must not build jobs"))

  "plan" should "return every declared contract of the source, in file order" in {
    val plan = ContractRunner.plan("clinvar", twoMajors, acceptAll)

    plan.jobs.map { case (c, _) => c.lineage } shouldBe List("1.4", "2.0")
    plan.tablePrefix shouldBe "clinvar"
  }

  it should "name the table each MAJOR publishes to" in {
    val plan = ContractRunner.plan("clinvar", twoMajors, acceptAll)

    plan.jobs.map { case (c, _) => ContractDestination.tableName(plan.tablePrefix, c.major) } shouldBe
      List("clinvar_v1", "clinvar_v2")
  }

  it should "reject a source with no declared contract" in {
    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("cosmic", twoMajors)
    ex.getMessage should include("No contract declared for source 'cosmic'")
  }

  it should "reject a table_prefix that already carries a MAJOR suffix" in {
    val contracts = Contracts.parse(
      """sources:
        |  clinvar:
        |    table_prefix: "clinvar_v1"
        |    contracts:
        |      - lineage: "1.0"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("clinvar", contracts, acceptAll)
    ex.getMessage should include("already carries a MAJOR suffix")
    ex.getMessage should include("'clinvar_v1'")
  }

  it should "reject two rows declaring the same MAJOR" in {
    val contracts = Contracts.parse(
      """sources:
        |  clinvar:
        |    table_prefix: "clinvar"
        |    contracts:
        |      - lineage: "1.0"
        |        release_notes: "v1.md"
        |      - lineage: "1.3"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("clinvar", contracts)
    ex.getMessage should include("same MAJOR more than once")
    ex.getMessage should include("'clinvar'")
    ex.getMessage should include("MAJOR 1: 1.0, 1.3")
  }

  it should "reject a MAJOR with no registry entry" in {
    val contracts = Contracts.parse(
      """sources:
        |  cosmic:
        |    table_prefix: "cosmic"
        |    contracts:
        |      - lineage: "1.0"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("cosmic", contracts)
    ex.getMessage should include("No ContractRegistry entry for source 'cosmic'")
    ex.getMessage should include("(cosmic, MAJOR 1)")
  }

  "destinationMismatchReason" should "accept a job writing to the table its MAJOR derives" in {
    val contract = Contract("1.0", "v1.md")
    ContractRunner.destinationMismatchReason("clinvar", contract, iceberg("normalized_clinvar_v1", Some("clinvar_v1"))) shouldBe None
  }

  it should "report a job writing to another MAJOR's table" in {
    val contract = Contract("2.0", "v2.md")
    val mismatch = ContractRunner.destinationMismatchReason("clinvar", contract, iceberg("normalized_clinvar_v1", Some("clinvar_v1")))

    mismatch.value should include("publishes to 'clinvar_v2'")
    mismatch.value should include("writes to 'clinvar_v1'")
  }

  it should "report a destination with no table at all" in {
    val contract = Contract("1.0", "v1.md")
    val mismatch = ContractRunner.destinationMismatchReason("clinvar", contract, iceberg("normalized_clinvar_v1", None))

    mismatch.value should include("writes to no table")
  }

  "plan" should "accept every source declared in the packaged contracts.yml" in {
    val contracts = Contracts.load()

    val failures = contracts.sourceNames.toList.sorted.flatMap { source =>
      Try(ContractRunner.plan(source, contracts)).failed.toOption.map(e => s"$source -> ${e.getMessage}")
    }

    failures shouldBe empty
  }

  "plan" should "validate against an injected factory lookup rather than the real registry" in {
    val contracts = Contracts.parse(
      """sources:
        |  fake:
        |    table_prefix: "fake"
        |    contracts:
        |      - lineage: "1.0"
        |        release_notes: "v1.md"
        |""".stripMargin
    )

    ContractRegistry.factory("fake", Contract("1.0", "v1.md")) shouldBe None
    an[IllegalArgumentException] should be thrownBy ContractRunner.plan("fake", contracts)

    ContractRunner.plan("fake", contracts, acceptAll).jobs.map { case (c, _) => c.lineage } shouldBe List("1.0")
  }
}
