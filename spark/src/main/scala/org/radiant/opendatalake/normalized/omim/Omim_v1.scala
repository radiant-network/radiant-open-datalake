package org.radiant.opendatalake.normalized.omim

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput
import org.radiant.opendatalake.normalized.omim.OmimPhenotype.parse_pheno

import java.time.LocalDateTime

case class Omim_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String)
  extends ContractETLP(rc, sourceDatasetId = "normalized_omim", tablePrefix, major = 1) {

  private val raw_omim_gene_set: DatasetConf = conf.getDataset("raw_omim_gene_set")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(raw_omim_gene_set.id -> RawInput.readVersioned(raw_omim_gene_set.id, version, rawStorage))

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val intermediateDf =
      data(raw_omim_gene_set.id)
        .select(
          col("_c0") as "chromosome",
          col("_c1") as "start",
          col("_c2") as "end",
          col("_c3") as "cypto_location",
          col("_c4") as "computed_cypto_location",
          col("_c5") as "omim_gene_id",
          split(col("_c6"), ", ") as "symbols",
          col("_c7") as "name",
          col("_c8") as "approved_symbol",
          col("_c9") as "entrez_gene_id",
          col("_c10") as "ensembl_gene_id",
          col("_c11") as "documentation",
          split(col("_c12"), ";") as "phenotypes"
        )

    val nullPhenotypes =
      intermediateDf
        .filter(col("phenotypes").isNull)
        .drop("phenotypes")
        .withColumn(
          "phenotype",
          lit(null).cast(
            "struct<name:string,omim_id:string,inheritance:array<string>,inheritance_code:array<string>>"
          )
        )

    intermediateDf
      .withColumn("raw_phenotype", explode(col("phenotypes")))
      .drop("phenotypes")
      .withColumn("phenotype", parse_pheno(col("raw_phenotype")))
      .drop("raw_phenotype")
      .unionByName(nullPhenotypes)
  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
