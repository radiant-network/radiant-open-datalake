package org.radiant.opendatalake.normalized.ensembl

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import org.apache.spark.sql.DataFrame
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

/** One row per (gene, exon) of the GFF3 release, with the set of transcripts the exon belongs to. */
case class EnsemblExonByGene_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, override val warehouse: Option[String] = None)
  extends ContractETLP(rc, sourceDatasetId = "normalized_ensembl_exon_by_gene", tablePrefix, major = 1, database) {

  private val raw_ensembl_gff3: DatasetConf = conf.getDataset("raw_ensembl_gff3")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(raw_ensembl_gff3.id -> RawInput.readVersioned(raw_ensembl_gff3.id, version, rawStorage))

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame =
    EnsemblGff.exonsByGene(EnsemblGff.exons(EnsemblGff.base(data(raw_ensembl_gff3.id))))

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
