package org.radiant.opendatalake.normalized.ensembl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Transformations shared by the four Ensembl contracts (gene, transcript, exon, exon_by_gene), all built from
 * one Ensembl GFF3 release read with Glow's `gff` reader.
 *
 * Glow reports `start` 0-based, so it is shifted back to the 1-based coordinate Ensembl publishes. The GFF
 * attributes arrive as columns: the GFF3-reserved ones keep their case (`Name`, `Alias`, `Parent`, the latter
 * two as arrays), every other attribute is a string column named as in the file.
 */
object EnsemblGff {

  def base(raw: DataFrame): DataFrame =
    raw
      .withColumn("start", col("start") + 1)
      .withColumnRenamed("seqId", "chromosome")
      .withColumn("tags", split(col("tag"), ","))
      .drop("tag")
      .withColumn("length", col("end") - col("start") + 1)

  def genes(base: DataFrame): DataFrame =
    base
      .where(col("gene_id").isNotNull)
      .select(
        col("chromosome"), col("start"), col("end"), col("gene_id"), col("version").cast("int") as "version",
        col("type"), col("strand"), col("phase"),
        col("Name") as "name", col("Alias") as "alias", col("biotype"), col("ccdsid"), col("constitutive"),
        col("description"), col("ensembl_end_phase"), col("ensembl_phase"), col("external_name"),
        col("logic_name"), col("length")
      )

  def transcripts(base: DataFrame): DataFrame =
    base
      .where(col("transcript_id").isNotNull)
      .withColumn("is_canonical", array_contains(col("tags"), "Ensembl_canonical"))
      .withColumn("is_mane_select", array_contains(col("tags"), "MANE_Select"))
      .withColumn("is_mane_plus_clinical", array_contains(col("tags"), "MANE_Plus_Clinical"))
      .withColumn("gene_id", regexp_replace(col("Parent")(0), "gene:", ""))
      .select(
        col("chromosome"), col("start"), col("end"), col("gene_id"), col("transcript_id"),
        col("version").cast("int") as "version", col("type"), col("strand"), col("phase"),
        col("is_canonical"), col("is_mane_select"), col("is_mane_plus_clinical"),
        col("Name") as "name", col("Alias") as "alias", col("biotype"), col("ccdsid"), col("constitutive"),
        col("description"), col("ensembl_end_phase"), col("ensembl_phase"), col("external_name"),
        col("logic_name"), col("rank"), col("transcript_support_level"), col("tags"), col("length")
      )

  /**
   * One row per (exon, transcript): an exon shared by several transcripts appears once for each.
   *
   * Ensembl exon rows carry only `Name`, `Parent`, `exon_id`, `version`, `rank`, `constitutive`,
   * `ensembl_phase` and `ensembl_end_phase`, and no GFF phase. Everything else would be null on every row, so
   * the transcript-level context (`gene_id`, the canonical / MANE flags, `tags`, `transcript_support_level`)
   * comes from the parent transcript, and the gene / region attributes are not carried at all.
   */
  def exons(base: DataFrame): DataFrame = {
    val parentTranscript =
      transcripts(base).select(
        "transcript_id", "gene_id", "is_mane_select", "is_canonical", "is_mane_plus_clinical",
        "transcript_support_level", "tags"
      )

    base
      .where(col("exon_id").isNotNull)
      .drop("gene_id", "transcript_support_level", "tags")
      .withColumn("transcript_id", regexp_replace(col("Parent")(0), "transcript:", ""))
      .join(parentTranscript, Seq("transcript_id"))
      .select(
        col("chromosome"), col("start"), col("end"), col("exon_id"), col("gene_id"), col("transcript_id"),
        col("version").cast("int") as "version", col("is_mane_select"), col("is_canonical"),
        col("is_mane_plus_clinical"), col("type"), col("strand"),
        col("Name") as "name", col("constitutive"), col("ensembl_end_phase"), col("ensembl_phase"), col("rank"),
        col("transcript_support_level"), col("tags"), col("length")
      )
  }

  /**
   * One row per (gene, exon), carrying every transcript the exon belongs to. Only exon-level attributes are
   * kept: the per-transcript ones (flags, `rank`, `tags`, `transcript_support_level`) differ across the
   * group, so `any_value` would pick one transcript's value arbitrarily.
   */
  def exonsByGene(exons: DataFrame): DataFrame =
    exons
      .groupBy("gene_id", "exon_id")
      .agg(
        any_value(col("chromosome")) as "chromosome", any_value(col("start")) as "start",
        any_value(col("end")) as "end", collect_set(col("transcript_id")) as "transcript_ids",
        any_value(col("version")) as "version", any_value(col("type")) as "type",
        any_value(col("strand")) as "strand", any_value(col("name")) as "name",
        any_value(col("constitutive")) as "constitutive",
        any_value(col("ensembl_end_phase")) as "ensembl_end_phase", any_value(col("ensembl_phase")) as "ensembl_phase",
        any_value(col("length")) as "length"
      )
}
