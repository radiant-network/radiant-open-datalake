package org.radiant.opendatalake.normalized.gnomad

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.array_contains
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime


// Here "v1" represents the opendatalake contract version (1.x.x).
// The raw dataset version compatible with this normalizer is 4.1, the latest gnomAD release
// carrying structural variants (4.1.1 has no `genome_sv/` directory).
case class GnomadSV_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String)
  extends ContractETLP(rc, sourceDatasetId = "normalized_gnomad_sv", tablePrefix, major = 1) {

  val gnomad_vcf: DatasetConf = conf.getDataset("raw_gnomad_sv")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(gnomad_vcf.id -> RawInput.readVersioned(gnomad_vcf.id, version, rawStorage))
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    // Only the high-quality calls are published, so `filters` itself is not: every published row
    // would carry the same value. The VCF reader returns FILTER as an array, since a record can
    // carry several filters at once; array_contains is how a PASS is recognised there.
    //
    // INFO_END duplicates the `end` column computed by the VCF reader, and flattening it would
    // produce two columns named `end`.
    val df = data(gnomad_vcf.id)
      .where(array_contains($"filters", "PASS"))
      .drop("INFO_END")

    val intermediate = df
      .select(
        chromosome +:
        start +:
        end +:
        reference +:
        alternate +:
        name +:
        flattenInfo(df): _*
      )

    intermediate.select(
      $"chromosome",
      $"start",
      $"end",
      $"reference",
      $"alternate",
      $"name",
      $"svtype",
      $"ac".cast("long"),
      $"an".cast("long"),
      $"af",
      $"n_het".cast("long"),
      $"n_homalt".cast("long"),
      $"n_bi_genos".cast("long")
    )
  }

  // ~1.2M PASS records over 13 columns: a handful of files per chromosome is plenty.
  override val defaultRepartition: DataFrame => DataFrame =
    RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(10))

}
