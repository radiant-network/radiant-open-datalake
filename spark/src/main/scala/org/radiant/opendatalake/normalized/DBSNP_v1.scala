package org.radiant.opendatalake.normalized


import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByColumns, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.radiant.opendatalake.normalized.io.RawInput
import org.radiant.opendatalake.wap.WapETLP

import java.time.LocalDateTime

case class DBSNP_v1(rc: RuntimeETLContext, version: String, rawStorage: String) extends WapETLP(rc)  {

  override val mainDestination: DatasetConf = conf.getDataset("normalized_dbsnp")

  private val raw_dbsnp = conf.getDataset("raw_dbsnp")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {
    Map(raw_dbsnp.id -> RawInput.readVersioned(raw_dbsnp.id, version, rawStorage))
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._
    data(raw_dbsnp.id)
      .where($"contigName" like "NC_%")
      .withColumn("alternate", explode($"alternateAlleles"))
      .withColumn("chromosome", regexp_extract($"contigName", "NC_(\\d+).(\\d+)", 1).cast("int"))
      .select(
        when($"chromosome" === 23, "X")
          .when($"chromosome" === 24, "Y")
          .when($"chromosome" === 12920, "M")
          .otherwise($"chromosome".cast("string")) as "chromosome",
        start,
        end,
        name,
        reference,
        $"alternate",
        $"contigName" as "original_contig_name"
      )
  }

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByColumns(columnNames = Seq("chromosome"), sortColumns = Seq("start"))

}
