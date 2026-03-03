package org.radiant.opendatalake

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import org.radiant.opendatalake.enriched.{DBNSFP, Genes, RareVariant}
import org.radiant.opendatalake.normalized._
import org.radiant.opendatalake.normalized.gnomad._
import org.radiant.opendatalake.normalized.omim.OmimGeneSet
import org.radiant.opendatalake.normalized.orphanet.OrphanetGeneSet
import org.radiant.opendatalake.normalized.refseq.{RefSeqAnnotation, RefSeqHumanGenes}
import mainargs.{ParserForMethods, main}

object ImportPublicTable {

  @main
  def annovar_scores(rc: RuntimeETLContext): Unit = AnnovarScores.run(rc)

  @main
  def clinvar(rc: RuntimeETLContext): Unit = Clinvar.run(rc)

  @main
  def dbnsfp_raw(rc: RuntimeETLContext): Unit = DBNSFPRaw.run(rc)

  @main
  def dbnsfp(rc: RuntimeETLContext): Unit = DBNSFP.run(rc)

  @main
  def dbsnp(rc: RuntimeETLContext): Unit = DBSNP.run(rc)

  @main
  def ddd(rc: RuntimeETLContext): Unit = DDDGeneSet.run(rc)

  @main
  def ensembl_mapping(rc: RuntimeETLContext): Unit = EnsemblMapping.run(rc)

  @main
  def gnomadv3(rc: RuntimeETLContext): Unit = GnomadV3.run(rc)

  @main
  def gnomadv4(rc: RuntimeETLContext): Unit = GnomadV4.run(rc)

  @main
  def gnomadv4cnv(rc: RuntimeETLContext): Unit = GnomadV4CNV.run(rc)

  @main
  def gnomadv4sv(rc: RuntimeETLContext): Unit = GnomadV4SV.run(rc)

  @main
  def gnomad_constraint(rc: RuntimeETLContext): Unit = GnomadConstraint.run(rc)

  @main
  def genes(rc: RuntimeETLContext): Unit = Genes.run(rc)

  @main
  def hpo(rc: RuntimeETLContext): Unit = HPOGeneSet.run(rc)

  @main
  def omim(rc: RuntimeETLContext): Unit = OmimGeneSet.run(rc)

  @main(name = "1000genomes")
  def one_thousand_genomes(rc: RuntimeETLContext): Unit = OneThousandGenomes.run(rc)

  @main
  def orphanet(rc: RuntimeETLContext): Unit = OrphanetGeneSet.run(rc)

  @main
  def refseq_annotation(rc: RuntimeETLContext): Unit = RefSeqAnnotation.run(rc)

  @main
  def refseq_human_genes(rc: RuntimeETLContext): Unit = RefSeqHumanGenes.run(rc)

  @main
  def spliceai_indel(rc: RuntimeETLContext): Unit = SpliceAi.run(rc, "indel")

  @main
  def spliceai_snv(rc: RuntimeETLContext): Unit = SpliceAi.run(rc, "snv")

  @main
  def spliceai_enriched_indel(rc: RuntimeETLContext): Unit = enriched.SpliceAi.run(rc, "indel")

  @main
  def spliceai_enriched_snv(rc: RuntimeETLContext): Unit = enriched.SpliceAi.run(rc, "snv")

  @main
  def topmed_bravo(rc: RuntimeETLContext): Unit = TopMed.run(rc)

  @main
  def rare_variant_enriched(rc: RuntimeETLContext): Unit = RareVariant.run(rc)

  def main(args: Array[String]): Unit = ParserForMethods(this).runOrThrow(args, allowPositional = true)

}


