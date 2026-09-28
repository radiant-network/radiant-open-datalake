package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, IdentityRepartition, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

case class TopMed_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, override val warehouse: Option[String] = None)
  extends ContractETLP(rc, sourceDatasetId = "normalized_topmed_bravo", tablePrefix, major = 1, database) {

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

    Locus.withLocusHash(
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
    )
  }

  /**
    * No repartition on purpose, though this is also the inherited default -- stated so it is not
    * "fixed" back. The table is partitioned by `chromosome`, so Iceberg (>= 1.2.0) defaults
    * `write.distribution-mode` to `hash` and Spark inserts that exchange LAST: anything we distribute
    * upstream is re-hashed onto one task per chromosome anyway. The previous
    * `RepartitionByColumns("chromosome", sortColumns = "start")` was therefore a second full shuffle of
    * the callset, whose lost map output cascaded into repeated `FetchFailedException`s, and its `start`
    * ordering did not survive that exchange either.
    *
    * To split a large chromosome across tasks and order files by `start`, set it on the table --
    * `ALTER TABLE <db>.topmed_bravo_v1 WRITE ORDERED BY chromosome, start` -- which moves Iceberg's one
    * exchange to `range`.
    */
  override def defaultRepartition: DataFrame => DataFrame = IdentityRepartition
}
