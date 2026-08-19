package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, sha2}
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.enriched.dbnsfp
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

case class DBNSFP_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, override val warehouse: Option[String] = None)
  extends ContractETLP(rc, sourceDatasetId = "normalized_dbnsfp", tablePrefix, major = 1, database) {

  private val raw_dbnsfp: DatasetConf = conf.getDataset("raw_dbnsfp")

  private[normalized] def withLocus(df: DataFrame): DataFrame =
    df.withColumn("locus", concat_ws("-", col("chromosome"), col("start"), col("reference"), col("alternate")))
      .withColumn("locus_hash", sha2(col("locus"), 256))


  private[normalized] def toPlatformColumns(df: DataFrame): DataFrame = {
    val renames = Map("#chr" -> "chromosome", "pos(1-based)" -> "start", "ref" -> "reference", "alt" -> "alternate")
    val missing = renames.keys.filterNot(df.columns.contains).toSeq.sorted
    require(missing.isEmpty, s"raw dbNSFP is missing expected column(s): ${missing.mkString(", ")}")
    renames.foldLeft(df) { case (acc, (from, to)) => acc.withColumnRenamed(from, to) }
  }

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(raw_dbnsfp.id -> RawInput.readVersioned(raw_dbnsfp.id, version, rawStorage))

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    withLocus(dbnsfp.transform(toPlatformColumns(data(raw_dbnsfp.id))))
  }

  override val defaultRepartition: DataFrame => DataFrame =
    RepartitionByColumns(columnNames = Seq("chromosome"), sortColumns = Seq("start"))
}
