package org.radiant.opendatalake

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import org.radiant.opendatalake.contracts.ContractRunner
import org.radiant.opendatalake.enriched.{DBNSFP, Genes}
import org.radiant.opendatalake.normalized._
import org.radiant.opendatalake.normalized.gnomad._
import org.radiant.opendatalake.normalized.omim.OmimGeneSet
import org.radiant.opendatalake.normalized.orphanet.OrphanetGeneSet
import org.radiant.opendatalake.normalized.refseq.{RefSeqAnnotation, RefSeqHumanGenes}
import org.radiant.opendatalake.mainutils.{RawStorage, Version}
import mainargs.{ParserForMethods, main}

object ImportPublicTable {

  @main
  def annovar_scores(rc: RuntimeETLContext): Unit = AnnovarScores.run(rc)

  @main
  def clinvar(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("clinvar", rc, version.value, rawStorage.value)

  @main
  def dbnsfp_raw(rc: RuntimeETLContext): Unit = DBNSFPRaw.run(rc)

  @main
  def dbnsfp(rc: RuntimeETLContext): Unit = DBNSFP.run(rc)

  @main
  def dbsnp(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("dbsnp", rc, version.value, rawStorage.value)

  @main
  def ddd(rc: RuntimeETLContext): Unit = DDDGeneSet.run(rc)

  @main
  def ensembl_mapping(rc: RuntimeETLContext): Unit = EnsemblMapping.run(rc)

  @main
  def gnomad_cnv(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("gnomad_cnv", rc, version.value, rawStorage.value)

  @main
  def gnomad_joint(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("gnomad_joint", rc, version.value, rawStorage.value)

  @main
  def gnomad_sv(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("gnomad_sv", rc, version.value, rawStorage.value)

  @main
  def gnomad_constraint(rc: RuntimeETLContext): Unit = GnomadConstraint.run(rc)

  @main
  def genes(rc: RuntimeETLContext): Unit = Genes.run(rc)

  @main
  def hpo(rc: RuntimeETLContext): Unit = HPOGeneSet.run(rc)

  @main
  def hpo_genes(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("hpo_genes", rc, version.value, rawStorage.value)

  @main
  def hpo_terms(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("hpo_terms", rc, version.value, rawStorage.value)

  @main
  def mondo(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("mondo", rc, version.value, rawStorage.value)

  @main
  def omim(rc: RuntimeETLContext): Unit = OmimGeneSet.run(rc)

  @main(name = "1000genomes")
  def one_thousand_genomes(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("1000_genomes", rc, version.value, rawStorage.value)

  @main
  def orphanet(rc: RuntimeETLContext): Unit = OrphanetGeneSet.run(rc)

  @main
  def refseq_annotation(rc: RuntimeETLContext): Unit = RefSeqAnnotation.run(rc)

  @main
  def refseq_human_genes(rc: RuntimeETLContext): Unit = RefSeqHumanGenes.run(rc)

  @main
  def spliceai(rc: RuntimeETLContext, version: Version, rawStorage: RawStorage): Unit =
    ContractRunner.run("spliceai", rc, version.value, rawStorage.value)

  @main
  def topmed_bravo(rc: RuntimeETLContext): Unit = TopMed.run(rc)

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)

}


