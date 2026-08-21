package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.SimpleConfiguration
import bio.ferlab.datalake.spark3.etl.v4.ETL
import org.radiant.opendatalake.config.Contract
import org.radiant.opendatalake.normalized.{Clinvar_v1, DBNSFP_v1, DBSNP_v1, DDD_v1, HpoGenes_v1, HpoTerms_v1, Mondo_v1, OneThousandGenomes_v1, SpliceAi_v1}
import org.radiant.opendatalake.normalized.gnomad.{GnomadCNV_v1, GnomadJoint_v1, GnomadSV_v1}
import org.radiant.opendatalake.normalized.orphanet.Orphanet_v1

import java.time.LocalDateTime


object ContractRegistry {

  type NormalizerETL = ETL[LocalDateTime, SimpleConfiguration]

  private val factories: Map[(String, Int), NormalizerArgs => NormalizerETL] = Map(
    ("1000_genomes", 1) -> (args => OneThousandGenomes_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("clinvar", 1) -> (args => Clinvar_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("dbnsfp", 1) -> (args => DBNSFP_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("dbsnp", 1) -> (args => DBSNP_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("ddd", 1) -> (args => DDD_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("gnomad_cnv", 1) -> (args => GnomadCNV_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("gnomad_joint", 1) -> (args => GnomadJoint_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("gnomad_sv", 1) -> (args => GnomadSV_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("hpo_genes", 1) -> (args => HpoGenes_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("hpo_terms", 1) -> (args => HpoTerms_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("mondo", 1) -> (args => Mondo_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("orphanet", 1) -> (args => Orphanet_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse)),
    ("spliceai", 1) -> (args => SpliceAi_v1(args.rc, args.version, args.rawStorage, args.tablePrefix, args.database, args.warehouse))
  )

  def factory(source: String, contract: Contract): Option[NormalizerArgs => NormalizerETL] =
    factories.get((source, contract.major))

  // For test purposes only
  def registeredKeys: Set[(String, Int)] = factories.keySet
}
