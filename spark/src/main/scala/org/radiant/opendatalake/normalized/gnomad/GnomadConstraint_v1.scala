package org.radiant.opendatalake.normalized.gnomad

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import org.apache.spark.sql.functions.{col, from_json, regexp_replace}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

// Here "v1" represents the opendatalake contract version (1.x.x).
// The raw dataset version compatible with this normalizer is gnomAD 4.1.2 (gnomad.v4.1.2.constraint_metrics.tsv.bgz);
// 4.1.1 has the identical header. Field meanings: release/4.1.1/constraint/gnomad.v4.1.1.gene_constraint_ht_README.md
// in the gnomad-public-us-east-1 bucket.
//
// - The source names its columns after the Hail struct paths (`lof.oe_ci.upper`). Each one is published with its
//   dots replaced by underscores (`lof_oe_ci_upper`), case kept, so the source README still maps 1:1.
// - Missing values are the literal `NA` (handled by the raw dataset's `nullValue`). Arrays are JSON text: flags are
//   `["no_exp_lof"]`, and the `gen_anc_*` arrays may carry `null` elements, so both go through `from_json`.
// - The file carries both Ensembl and RefSeq transcripts. RefSeq rows have no locus (chromosome/start/end are NA),
//   so only Ensembl (`ENST`) rows are kept: the table is partitioned and range-repartitioned on the locus.
// - The `gen_anc_*` arrays are indexed by downsampling level; the levels themselves are a Hail Table global that
//   the TSV does not carry.
case class GnomadConstraint_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, override val warehouse: Option[String] = None)
  extends ContractETLP(rc, sourceDatasetId = "normalized_gnomad_constraint", tablePrefix, major = 1, database) {

  val raw_gnomad_constraint: DatasetConf = conf.getDataset("raw_gnomad_constraint")

  private val EnsemblTranscriptPrefix = "ENST"
  private val GeneticAncestryGroups = Seq("global", "afr", "amr", "eas", "nfe", "sas")
  private val Categories = Seq("syn", "mis", "lof_hc_lc", "lof")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(raw_gnomad_constraint.id -> RawInput.readVersioned(raw_gnomad_constraint.id, version, rawStorage))
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    data(raw_gnomad_constraint.id)
      .filter(raw("transcript").startsWith(EnsemblTranscriptPrefix))
      .select(identityColumns ++ qualityColumns ++ Categories.flatMap(categoryColumns): _*)
  }

  override def defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1))

  private def raw(name: String): Column = col(s"`$name`")

  private def typed(name: String, dataType: DataType): Column = raw(name).cast(dataType) as name.replace('.', '_')

  private def jsonArray(name: String, elementType: DataType): Column =
    from_json(raw(name), ArrayType(elementType)) as name.replace('.', '_')

  private def identityColumns: Seq[Column] = Seq(
    regexp_replace(raw("chromosome"), "^chr", "") as "chromosome",
    raw("start_position").cast(LongType) as "start",
    raw("end_position").cast(LongType) as "end",
    raw("gene") as "symbol",
    raw("gene_id"),
    raw("transcript"),
    raw("transcript_version"),
    raw("transcript_type"),
    typed("transcript_level", IntegerType),
    typed("canonical", BooleanType),
    typed("mane_select", BooleanType),
    typed("cds_length", LongType),
    typed("num_coding_exons", IntegerType)
  )

  private def qualityColumns: Seq[Column] = Seq(
    typed("gene_quality_metrics.exome_prop_bp_AN90", FloatType),
    typed("gene_quality_metrics.exome_mean_AS_MQ", FloatType),
    typed("gene_quality_metrics.exome_prop_segdup", FloatType),
    typed("gene_quality_metrics.exome_prop_LCR", FloatType),
    jsonArray("gene_flags", StringType),
    jsonArray("constraint_flags", StringType)
  )

  // Same column order as the source file within each category.
  private def categoryColumns(c: String): Seq[Column] = {
    val metrics = Seq(
      typed(s"$c.mu", FloatType),
      typed(s"$c.possible", LongType),
      typed(s"$c.obs", IntegerType),
      typed(s"$c.exp", FloatType),
      typed(s"$c.oe", FloatType),
      typed(s"$c.z_raw", FloatType),
      typed(s"$c.z_score", FloatType),
      typed(s"$c.oe_ci.lower", FloatType),
      typed(s"$c.oe_ci.upper", FloatType)
    )

    // LOEUF rank and bins are only computed on high-confidence pLoF.
    val loeufBins =
      if (c == "lof") Seq(
        typed("lof.oe_ci.upper_rank", IntegerType),
        typed("lof.oe_ci.upper_bin_percentile", IntegerType),
        typed("lof.oe_ci.upper_bin_decile", IntegerType),
        typed("lof.oe_ci.upper_bin_sextile", IntegerType)
      )
      else Nil

    val observed = GeneticAncestryGroups.map(g => jsonArray(s"$c.gen_anc_obs.$g", IntegerType))
    val expected = GeneticAncestryGroups.map(g => jsonArray(s"$c.gen_anc_exp.$g", DoubleType))

    val lofProbabilities =
      if (c.startsWith("lof")) Seq(typed(s"$c.pLI", FloatType), typed(s"$c.pNull", DoubleType), typed(s"$c.pRec", DoubleType))
      else Nil

    metrics ++ loeufBins ++ observed ++ expected ++ lofProbabilities
  }
}
