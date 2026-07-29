package org.radiant.opendatalake.config.contracts

import bio.ferlab.datalake.commons.config.{RuntimeETLContext, SimpleConfiguration}
import bio.ferlab.datalake.spark3.etl.v4.ETL
import org.radiant.opendatalake.normalized.{Clinvar_v1, DBSNP_v1}

import java.time.LocalDateTime


case class NormalizerArgs(rc: RuntimeETLContext, version: String, rawStorage: String)

object ContractRegistry {

  type Normalizer = ETL[LocalDateTime, SimpleConfiguration]

  private val factories: Map[(String, Int), NormalizerArgs => Normalizer] = Map(
    ("clinvar", 1) -> (a => Clinvar_v1(a.rc, a.version, a.rawStorage)),
    ("dbsnp", 1) -> (a => DBSNP_v1(a.rc, a.version, a.rawStorage))
  )

  def factory(contract: Contract): Option[NormalizerArgs => Normalizer] =
    factories.get((contract.table, contract.major))

  def known: Set[(String, Int)] = factories.keySet
}
