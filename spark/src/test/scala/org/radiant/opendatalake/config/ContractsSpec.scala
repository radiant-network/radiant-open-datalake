package org.radiant.opendatalake.config

import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.radiant.opendatalake.contracts.ContractRunner
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class ContractsSpec extends AnyFlatSpec with Matchers {

  "Contracts.load" should "parse the packaged contracts.yml from the classpath" in {
    val contracts = Contracts.load()

    contracts.sourceNames should contain allOf ("clinvar", "dbsnp", "gnomad_joint")

    contracts.forSource("clinvar") should contain(
      Contract(lineage = "1.0", releaseNotes = "doc/release-notes/clinvar/v1.md")
    )

    contracts.forSource("dbsnp") should contain(
      Contract(lineage = "1.0", releaseNotes = "doc/release-notes/dbsnp/v1.md")
    )

    contracts.forSource("gnomad_joint") should contain(
      Contract(lineage = "1.0", releaseNotes = "doc/release-notes/gnomad_joint/v1.md")
    )

    contracts.tablePrefixOf("clinvar") shouldBe Some("clinvar")
    contracts.tablePrefixOf("dbsnp") shouldBe Some("dbsnp")
    contracts.tablePrefixOf("gnomad_joint") shouldBe Some("gnomad_joint")
    contracts.tablePrefixOf("absent") shouldBe None
  }

  it should "throw a clear error when the resource is missing" in {
    val ex = the[IllegalArgumentException] thrownBy Contracts.load("/does-not-exist.yml")
    ex.getMessage should include("/does-not-exist.yml")
  }

  "Contracts.parse" should "map snake_case keys to camelCase fields and preserve file order" in {
    val yaml =
      """sources:
        |  clinvar:
        |    table_prefix: "clinvar"
        |    contracts:
        |      - lineage: "1.0"
        |        release_notes: "doc/release-notes/clinvar/v1.md"
        |      - lineage: "2.3"
        |        release_notes: "doc/release-notes/clinvar/v2.md"
        |""".stripMargin

    val contracts = Contracts.parse(yaml)
    val clinvar = contracts.forSource("clinvar")

    clinvar.map(_.lineage) shouldBe List("1.0", "2.3")
    clinvar.head.releaseNotes shouldBe "doc/release-notes/clinvar/v1.md" // release_notes -> releaseNotes
    contracts.tablePrefixOf("clinvar") shouldBe Some("clinvar") // table_prefix -> tablePrefix
  }

  it should "reject a contract row that names a table" in {
    val yaml =
      """sources:
        |  clinvar:
        |    table_prefix: "clinvar"
        |    contracts:
        |      - lineage: "1.0"
        |        table: "clinvar_v1"
        |        release_notes: "v1.md"
        |""".stripMargin

    an[UnrecognizedPropertyException] should be thrownBy Contracts.parse(yaml)
  }

  "Contract" should "derive MAJOR and MINOR from the lineage" in {
    val c = Contract("2.3", "notes.md")
    c.major shouldBe 2
    c.minor shouldBe 3
  }

  it should "reject a lineage that is not '{MAJOR}.{MINOR}' with numeric parts" in {
    an[IllegalArgumentException] should be thrownBy Contract("1", "r")
    an[IllegalArgumentException] should be thrownBy Contract("1.0.0", "r")
    an[IllegalArgumentException] should be thrownBy Contract("1.x", "r")
  }

  "Contracts.forSource" should "expose every declared MAJOR of a source and nothing for an absent one" in {
    val yaml =
      """sources:
        |  clinvar:
        |    table_prefix: "clinvar"
        |    contracts:
        |      - lineage: "1.4"
        |        release_notes: "v1.md"
        |      - lineage: "2.0"
        |        release_notes: "v2.md"
        |""".stripMargin

    val contracts = Contracts.parse(yaml)
    contracts.forSource("clinvar").map(_.major) shouldBe List(1, 2)
    contracts.forSource("absent") shouldBe empty
  }

  it should "fail loudly on an unknown field" in {
    val yaml =
      """sources:
        |  clinvar:
        |    table_prefix: "clinvar"
        |    contracts:
        |      - lineage: "1.0"
        |        release_notes: "v1.md"
        |        typo_field: "boom"
        |""".stripMargin

    an[UnrecognizedPropertyException] should be thrownBy Contracts.parse(yaml)
  }

  /*
    Jackson leaves absent or empty yaml keys as null. jackson-module-scala maps null -> None for the
    Option fields, but a null map *value* (`clinvar:` with nothing under it) stays null. Contracts drops
    those entries — with a required tablePrefix there is no empty SourceContracts to stand in for one — so
    such a source reaches ContractRunner's "No contract declared" message instead of a bare NPE.
  */
  it should "treat a null sources map or a null source as nothing declared" in {
    Contracts.parse("sources:\n").sourceNames shouldBe empty
    Contracts.parse("sources:\n").forSource("clinvar") shouldBe empty

    val emptySource = Contracts.parse("sources:\n  clinvar:\n")
    emptySource.sourceNames shouldBe empty
    emptySource.forSource("clinvar") shouldBe empty
    emptySource.tablePrefixOf("clinvar") shouldBe None

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("clinvar", emptySource)
    ex.getMessage should include("No contract declared for source 'clinvar'")
  }

  /*
    table_prefix is what names the table, so a source without one has nothing to publish to. Required at the
    type level, which makes it a parse failure — Jackson wraps the case-class require, same as for lineage.
  */
  it should "reject a source that declares no table_prefix" in {
    val yaml =
      """sources:
        |  clinvar:
        |    contracts:
        |      - lineage: "1.0"
        |        release_notes: "v1.md"
        |""".stripMargin

    val ex = the[JsonMappingException] thrownBy Contracts.parse(yaml)
    ex.getMessage should include("Missing table_prefix")
  }

  it should "reject an empty table_prefix" in {
    val yaml =
      """sources:
        |  clinvar:
        |    table_prefix: ""
        |    contracts:
        |      - lineage: "1.0"
        |        release_notes: "v1.md"
        |""".stripMargin

    val ex = the[JsonMappingException] thrownBy Contracts.parse(yaml)
    ex.getMessage should include("Missing table_prefix")
  }

  /*
    The lineage `require` runs inside the case-class body, so at parse time Jackson wraps it: callers
    of `load`/`parse` see a JsonMappingException, not the IllegalArgumentException the direct
    constructor throws. Pinning that here so the wrapping is a decision, not a surprise, and so the
    offending lineage stays in the message.
  */
  it should "surface a malformed lineage in the yaml with the bad value in the message" in {
    val yaml =
      """sources:
        |  clinvar:
        |    table_prefix: "clinvar"
        |    contracts:
        |      - lineage: "1.x"
        |        release_notes: "v1.md"
        |""".stripMargin

    val ex = the[JsonMappingException] thrownBy Contracts.parse(yaml)
    ex.getMessage should include("1.x")
  }

  /*
    `release_notes` is the one untyped path left in the file: one that never existed, or one left
    behind by a doc move, is invisible otherwise. Paths are relative to the spark module, which is the
    working directory of the forked test JVM (sbt `baseDirectory`).
  */
  it should "point every contract at a release notes file that exists" in {
    val contracts = Contracts.load()

    val missing = contracts.sourceNames.toList.sorted.flatMap { source =>
      contracts.forSource(source).collect {
        case c if !new File(c.releaseNotes).isFile => s"$source ${c.lineage} -> ${c.releaseNotes}"
      }
    }

    withClue(s"release notes declared in contracts.yml but absent from ${new File(".").getAbsolutePath}: ") {
      missing shouldBe empty
    }
  }
}
