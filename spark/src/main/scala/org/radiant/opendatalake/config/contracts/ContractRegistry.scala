package org.radiant.opendatalake.config.contracts

import bio.ferlab.datalake.commons.config.{RuntimeETLContext, SimpleConfiguration}
import bio.ferlab.datalake.spark3.etl.v4.ETL
import org.radiant.opendatalake.normalized.{Clinvar, DBSNP}

import java.time.LocalDateTime


case class NormalizerArgs(rc: RuntimeETLContext, version: String, rawStorage: String)

object ContractRegistry {

  type Normalizer = ETL[LocalDateTime, SimpleConfiguration]

  private val factories: Map[String, NormalizerArgs => Normalizer] = Map(
    classOf[Clinvar].getName -> (a => Clinvar(a.rc, a.version, a.rawStorage)),
    classOf[DBSNP].getName -> (a => DBSNP(a.rc, a.version, a.rawStorage))
  )

  /** Factory for a declared FQCN, if this build knows how to construct it. */
  def factory(normalizer: String): Option[NormalizerArgs => Normalizer] = factories.get(normalizer)

  /** FQCNs this build can construct. */
  def known: Set[String] = factories.keySet
}
