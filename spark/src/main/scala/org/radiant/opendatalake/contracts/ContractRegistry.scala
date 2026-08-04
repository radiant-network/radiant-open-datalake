package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.SimpleConfiguration
import bio.ferlab.datalake.spark3.etl.v4.ETL
import org.radiant.opendatalake.config.contracts.Contract
import org.radiant.opendatalake.normalized.{Clinvar_v1, DBSNP_v1}

import java.time.LocalDateTime


object ContractRegistry {

  type NormalizerETL = ETL[LocalDateTime, SimpleConfiguration]

  private val factories: Map[(String, Int), NormalizerArgs => NormalizerETL] = Map(
    ("clinvar", 1) -> (args => Clinvar_v1(args.rc, args.version, args.rawStorage, args.tablePrefix)),
    ("dbsnp", 1) -> (args => DBSNP_v1(args.rc, args.version, args.rawStorage, args.tablePrefix))
  )

  def factory(source: String, contract: Contract): Option[NormalizerArgs => NormalizerETL] =
    factories.get((source, contract.major))

  // For test purposes only
  def registeredKeys: Set[(String, Int)] = factories.keySet
}
