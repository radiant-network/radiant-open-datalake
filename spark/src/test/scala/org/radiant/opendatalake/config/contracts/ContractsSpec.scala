package org.radiant.opendatalake.config.contracts

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.etl.v4.ETL
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.lang.reflect.Modifier
import scala.util.Try

class ContractsSpec extends AnyFlatSpec with Matchers {

  "Contracts.load" should "parse the packaged contracts.yml from the classpath" in {
    val contracts = Contracts.load()

    contracts.sourceNames should contain allOf ("clinvar", "dbsnp")

    val clinvar = contracts.forSource("clinvar")
    clinvar should have size 1
    clinvar.head shouldBe Contract(
      lineage = "1.0",
      table = "clinvar",
      normalizer = "org.radiant.opendatalake.normalized.Clinvar",
      releaseNotes = "doc/release-notes/clinvar/v1.md"
    )

    contracts.forSource("dbsnp").head.normalizer shouldBe "org.radiant.opendatalake.normalized.DBSNP"
  }

  it should "declare a normalizer FQCN that resolves on the classpath for every contract" in {
    val contracts = Contracts.load()
    val loader = getClass.getClassLoader

    val unresolvable = contracts.sourceNames.toList.sorted.flatMap { source =>
      contracts.forSource(source).collect {
        case c if Try(Class.forName(c.normalizer, false, loader)).isFailure =>
          s"$source ${c.lineage} -> ${c.normalizer}"
      }
    }

    withClue("normalizer classes declared in contracts.yml but absent from the classpath: ") {
      unresolvable shouldBe empty
    }
  }

  it should "point every contract at a concrete ETL class taking a RuntimeETLContext" in {
    val contracts = Contracts.load()
    val loader = getClass.getClassLoader
    val etlBase = classOf[ETL[_, _]]
    val contextParam = classOf[RuntimeETLContext]

    val violations = contracts.sourceNames.toList.sorted.flatMap { source =>
      contracts.forSource(source).flatMap { c =>
        val label = s"$source ${c.lineage} -> ${c.normalizer}"
        // Unresolvable names are already reported by the test above; skip them to avoid double noise.
        Try(Class.forName(c.normalizer, false, loader)).toOption.flatMap { cls =>
          if (cls.isInterface || Modifier.isAbstract(cls.getModifiers))
            Some(s"$label: abstract or trait, cannot be instantiated")
          else if (!etlBase.isAssignableFrom(cls))
            Some(s"$label: does not extend ${etlBase.getName}")
          else if (!cls.getConstructors.exists(_.getParameterTypes.headOption.contains(contextParam)))
            Some(s"$label: no public constructor whose first parameter is a RuntimeETLContext")
          else None
        }
      }
    }

    withClue("normalizers declared in contracts.yml that the fan-out could not run: ") {
      violations shouldBe empty
    }
  }

  it should "throw a clear error when the resource is missing" in {
    val ex = the[IllegalArgumentException] thrownBy Contracts.load("/does-not-exist.yml")
    ex.getMessage should include("/does-not-exist.yml")
  }

  "Contracts.parse" should "map snake_case keys to camelCase fields and preserve file order" in {
    val yaml =
      """sources:
        |  clinvar:
        |    contracts:
        |      - lineage: "1.0"
        |        table: "clinvar_v1"
        |        normalizer: "org.radiant.opendatalake.normalized.clinvar.clinvar_v1"
        |        release_notes: "doc/release-notes/clinvar/v1.md"
        |      - lineage: "2.3"
        |        table: "clinvar_v2"
        |        normalizer: "org.radiant.opendatalake.normalized.clinvar.clinvar_v2"
        |        release_notes: "doc/release-notes/clinvar/v2.md"
        |""".stripMargin

    val contracts = Contracts.parse(yaml)
    val clinvar = contracts.forSource("clinvar")

    clinvar.map(_.lineage) shouldBe List("1.0", "2.3")
    clinvar.head.releaseNotes shouldBe "doc/release-notes/clinvar/v1.md" // release_notes -> releaseNotes
  }

  "Contract" should "derive MAJOR and MINOR from the lineage" in {
    val c = Contract("2.3", "clinvar_v2", "com.example.Norm", "notes.md")
    c.major shouldBe 2
    c.minor shouldBe 3
  }

  it should "reject a lineage that is not '{MAJOR}.{MINOR}' with numeric parts" in {
    an[IllegalArgumentException] should be thrownBy Contract("1", "t", "n", "r")
    an[IllegalArgumentException] should be thrownBy Contract("1.0.0", "t", "n", "r")
    an[IllegalArgumentException] should be thrownBy Contract("1.x", "t", "n", "r")
  }

  "Contracts.forSource" should "expose every declared MAJOR of a source and nothing for an absent one" in {
    val yaml =
      """sources:
        |  clinvar:
        |    contracts:
        |      - lineage: "1.4"
        |        table: "clinvar_v1"
        |        normalizer: "com.example.V1"
        |        release_notes: "v1.md"
        |      - lineage: "2.0"
        |        table: "clinvar_v2"
        |        normalizer: "com.example.V2"
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
        |    contracts:
        |      - lineage: "1.0"
        |        table: "clinvar"
        |        normalizer: "com.example.Norm"
        |        release_notes: "v1.md"
        |        typo_field: "boom"
        |""".stripMargin

    an[UnrecognizedPropertyException] should be thrownBy Contracts.parse(yaml)
  }

  it should "treat a null sources map or a null contracts list as nothing declared" in {
    Contracts.parse("sources:\n").sourceNames shouldBe empty
    Contracts.parse("sources:\n").forSource("clinvar") shouldBe empty

    val sourceWithoutList = Contracts.parse("sources:\n  clinvar:\n")
    sourceWithoutList.sourceNames should contain("clinvar")
    sourceWithoutList.forSource("clinvar") shouldBe empty

    val ex = the[IllegalArgumentException] thrownBy ContractRunner.plan("clinvar", sourceWithoutList)
    ex.getMessage should include("No contract declared for source 'clinvar'")
  }

  it should "surface a malformed lineage in the yaml with the bad value in the message" in {
    val yaml =
      """sources:
        |  clinvar:
        |    contracts:
        |      - lineage: "1.x"
        |        table: "clinvar"
        |        normalizer: "com.example.Norm"
        |        release_notes: "v1.md"
        |""".stripMargin

    val ex = the[JsonMappingException] thrownBy Contracts.parse(yaml)
    ex.getMessage should include("1.x")
  }

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
