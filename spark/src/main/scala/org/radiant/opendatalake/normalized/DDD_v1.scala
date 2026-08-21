package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

case class DDD_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, override val warehouse: Option[String] = None)
  extends ContractETLP(rc, sourceDatasetId = "normalized_ddd", tablePrefix, major = 1, database) {

  import spark.implicits._

  private val raw_ddd_gene_set: DatasetConf = conf.getDataset("raw_ddd_gene_set")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(raw_ddd_gene_set.id -> RawInput.readVersioned(raw_ddd_gene_set.id, version, rawStorage))

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame =
    data(raw_ddd_gene_set.id)
      .select(
        $"gene symbol" as "symbol",
        $"gene mim" as "omim_gene_id",
        $"disease name" as "disease_name",
        $"disease mim" as "disease_omim_id",
        $"confidence" as "confidence_category",
        $"variant consequence" as "mutation_consequence",
        split($"variant types", "; ") as "variant_consequence",
        split($"phenotypes", "; ") as "phenotypes",
        $"panel",
        $"hgnc id" as "hgnc_id"
      )

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
