package org.radiant.opendatalake.contracts

import org.radiant.opendatalake.config.{Contract, Contracts}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ContractRegistrySpec extends AnyFlatSpec with Matchers {

  private val contracts: Contracts = Contracts.load()

  private def declared: Set[(String, Int)] =
    contracts.sourceNames.flatMap(source => contracts.forSource(source).map(c => (source, c.major)))

  "ContractRegistry" should "provide a factory for every contract declared in contracts.yml" in {
    val missing = contracts.sourceNames.toList.sorted.flatMap { source =>
      contracts.forSource(source).collect {
        case c if ContractRegistry.factory(source, c).isEmpty => s"$source ${c.lineage} -> ($source, MAJOR ${c.major})"
      }
    }

    withClue("contracts declared in contracts.yml that ContractRegistry cannot build: ") {
      missing shouldBe empty
    }
  }

  it should "key a factory on the MAJOR as well as the source" in {
    ContractRegistry.factory("clinvar", Contract("1.0", "v1.md")) should not be empty
    ContractRegistry.factory("clinvar", Contract("99.0", "v99.md")) shouldBe None
    ContractRegistry.factory("cosmic", Contract("1.0", "v1.md")) shouldBe None
  }

  it should "not register a (source, MAJOR) that no contract declares" in {
    withClue("registered but undeclared — dead entry, or a missing contracts.yml row: ") {
      ContractRegistry.registeredKeys.diff(declared) shouldBe empty
    }
  }
}
