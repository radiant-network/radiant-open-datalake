package org.radiant.opendatalake.config.contracts

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ContractRegistrySpec extends AnyFlatSpec with Matchers {

  "ContractRegistry" should "provide a factory for every contract declared in contracts.yml" in {
    val contracts = Contracts.load()

    val missing = contracts.sourceNames.toList.sorted.flatMap { source =>
      contracts.forSource(source).collect {
        case c if ContractRegistry.factory(c).isEmpty => s"$source ${c.lineage} -> (${c.table}, MAJOR ${c.major})"
      }
    }

    withClue("contracts declared in contracts.yml that ContractRegistry cannot build: ") {
      missing shouldBe empty
    }
  }

  it should "not register a (table, MAJOR) that no contract declares" in {
    val contracts = Contracts.load()
    val declared = contracts.sourceNames.flatMap(contracts.forSource(_).map(c => (c.table, c.major)))

    withClue("registered but undeclared — dead entry, or a missing contracts.yml row: ") {
      ContractRegistry.known.diff(declared) shouldBe empty
    }
  }
}
