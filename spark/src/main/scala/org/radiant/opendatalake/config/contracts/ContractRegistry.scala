package org.radiant.opendatalake.config.contracts

import bio.ferlab.datalake.commons.config.SimpleConfiguration
import bio.ferlab.datalake.spark3.etl.v4.ETL
import org.radiant.opendatalake.normalized.{Clinvar_v1, DBSNP_v1}

import java.time.LocalDateTime


object ContractRegistry {

  type NormalizerETL = ETL[LocalDateTime, SimpleConfiguration]

  private val factories: Map[(String, Int), NormalizerArgs => NormalizerETL] = Map(
    ("clinvar", 1) -> (args => Clinvar_v1(args.rc, args.version, args.rawStorage)),
    ("dbsnp", 1) -> (args => DBSNP_v1(args.rc, args.version, args.rawStorage))
  )

  def factory(contract: Contract): Option[NormalizerArgs => NormalizerETL] =
    factories.get((contract.table, contract.major))

  def known: Set[(String, Int)] = factories.keySet
}
