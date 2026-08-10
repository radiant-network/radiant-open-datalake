package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

case class TopMed_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String)
  extends ContractETLP(rc, sourceDatasetId = "normalized_topmed_bravo", tablePrefix, major = 1) {

  val raw_topmed: DatasetConf = conf.getDataset("raw_topmed_bravo")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] =
    Map(raw_topmed.id -> RawInput.readVersioned(raw_topmed.id, version, rawStorage))

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame = {
    import spark.implicits._

    val topmedDataFrame = data(raw_topmed.id)
    // Freeze 8 ships INFO_AN; freeze 10 dropped it, so recover it from AC / AF.
    val topmedDataFrameWithAnColumn: DataFrame = if (topmedDataFrame.columns.contains("INFO_AN")) topmedDataFrame
                                                 else topmedDataFrame.withColumn("INFO_AN", lit(round(ac / af)).cast(IntegerType))

    topmedDataFrameWithAnColumn.select(
        chromosome,
        start,
        end,
        name,
        reference,
        alternate,
        ac,
        af,
        an,
        $"INFO_HOM"(0) as "homozygotes",
        $"INFO_HET"(0) as "heterozygotes",
        $"qual",
        when(size($"filters") === 1 && $"filters"(0) === "PASS", "PASS")
          .when(array_contains($"filters", "PASS"), "PASS+FAIL")
          .otherwise("FAIL") as "qual_filter"
      )
  }

  override def defaultRepartition: DataFrame => DataFrame =
    RepartitionByColumns(columnNames = Seq("chromosome"), sortColumns = Seq("start"))
}
