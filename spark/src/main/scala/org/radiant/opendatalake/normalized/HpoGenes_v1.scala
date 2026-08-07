package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

case class HpoGene(entrez_gene_id: Long,
                   symbol: String,
                   hpo_term_id: String,
                   hpo_term_name: String,
                   frequency: Option[String],
                   disease_id: String)

case class HpoGenes_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String)
  extends ContractETLP(rc, sourceDatasetId = "normalized_hpo_genes", tablePrefix, major = 1) {

  private val raw_hpo_genes: DatasetConf = conf.getDataset("raw_hpo_genes")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(raw_hpo_genes.id -> RawInput.readVersioned(raw_hpo_genes.id, version, rawStorage))

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame =
    // genes_to_phenotype.txt (HPO releases): ncbi_gene_id, gene_symbol, hpo_id, hpo_name, frequency, disease_id.
    // Renamed to the schema enriched.Genes.withHPO expects (entrez_gene_id, hpo_term_id, hpo_term_name).
    data(raw_hpo_genes.id)
      .select(
        col("ncbi_gene_id").cast("long").as("entrez_gene_id"),
        col("gene_symbol").as("symbol"),
        col("hpo_id").as("hpo_term_id"),
        col("hpo_name").as("hpo_term_name"),
        col("frequency"),
        col("disease_id")
      )

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
