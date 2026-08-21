package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.enriched.{SpliceAi => EnrichedSpliceAi}
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

case class SpliceAi_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, override val warehouse: Option[String] = None)
  extends ContractETLP(rc, sourceDatasetId = "normalized_spliceai", tablePrefix, major = 1, database) {

  val raw_spliceai: DatasetConf = conf.getDataset("raw_spliceai")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] =
    Map(raw_spliceai.id -> RawInput.readVersioned(raw_spliceai.id, version, rawStorage))

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame =
    EnrichedSpliceAi.addMaxScore(normalize(data(raw_spliceai.id)))

  override def defaultRepartition: DataFrame => DataFrame =
    RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1000))

  private def normalize(df: DataFrame): DataFrame =
    df
      .select(
        chromosome +:
          start +:
          end +:
          reference +:
          alternate +:
          explode(col("INFO_SpliceAI")).as("spliceai") +:
          flattenInfo(df, "INFO_SpliceAI", "INFO_OLD_MULTIALLELIC", "INFO_FILTERS"): _*
      )
      .withColumn("spliceai", split(col("spliceai"), "\\|"))
      .withColumn("allele", col("spliceai")(0))
      .withColumn("symbol", col("spliceai")(1))
      .withColumn("ds_ag", col("spliceai")(2).cast("double"))
      .withColumn("ds_al", col("spliceai")(3).cast("double"))
      .withColumn("ds_dg", col("spliceai")(4).cast("double"))
      .withColumn("ds_dl", col("spliceai")(5).cast("double"))
      .withColumn("dp_ag", col("spliceai")(6).cast("int"))
      .withColumn("dp_al", col("spliceai")(7).cast("int"))
      .withColumn("dp_dg", col("spliceai")(8).cast("int"))
      .withColumn("dp_dl", col("spliceai")(9).cast("int"))
      .drop("spliceai")
}
