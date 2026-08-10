package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import org.apache.spark.sql.DataFrame
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

case class HpoGenes_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String)
  extends ContractETLP(rc, sourceDatasetId = "normalized_hpo_genes", tablePrefix, major = 1) {

  private val raw_hpo_genes: DatasetConf = conf.getDataset("raw_hpo_genes")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(raw_hpo_genes.id -> RawInput.readVersioned(raw_hpo_genes.id, version, rawStorage))

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame =
    data(raw_hpo_genes.id)

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
