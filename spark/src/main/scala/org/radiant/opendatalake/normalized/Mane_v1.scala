package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

/** One row per MANE transcript (MANE Select or MANE Plus Clinical) of the NCBI MANE summary file. */
case class Mane_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, override val warehouse: Option[String] = None)
  extends ContractETLP(rc, sourceDatasetId = "normalized_mane", tablePrefix, major = 1, database) {

  private val raw_mane_summary: DatasetConf = conf.getDataset("raw_mane_summary")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(raw_mane_summary.id -> RawInput.readVersioned(raw_mane_summary.id, version, rawStorage))

  // Versioned ids are split so they join on (id, version) as they are, e.g. ensembl_transcript_v1 on (transcript_id, version).
  private def accession(versioned: Column): Column = substring_index(versioned, ".", 1)
  private def accessionVersion(versioned: Column): Column = substring_index(versioned, ".", -1).cast("int")

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame =
    data(raw_mane_summary.id)
      .select(
        regexp_replace(col("#NCBI_GeneID"), "^GeneID:", "").cast("int") as "ncbi_gene_id",
        accession(col("Ensembl_Gene")) as "ensembl_gene_id",
        accessionVersion(col("Ensembl_Gene")) as "ensembl_gene_version",
        col("HGNC_ID") as "hgnc_id",
        col("symbol"),
        col("name"),
        col("RefSeq_nuc") as "refseq_transcript_id",
        accession(col("RefSeq_nuc")) as "refseq_transcript_accession",
        accessionVersion(col("RefSeq_nuc")) as "refseq_transcript_version",
        col("RefSeq_prot") as "refseq_protein_id",
        accession(col("Ensembl_nuc")) as "ensembl_transcript_id",
        accessionVersion(col("Ensembl_nuc")) as "ensembl_transcript_version",
        col("Ensembl_prot") as "ensembl_protein_id",
        col("MANE_status") as "mane_status",
        col("GRCh38_chr") as "chromosome_accession",
        col("chr_start").cast("long") as "start",
        col("chr_end").cast("long") as "end",
        col("chr_strand") as "strand"
      )

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
