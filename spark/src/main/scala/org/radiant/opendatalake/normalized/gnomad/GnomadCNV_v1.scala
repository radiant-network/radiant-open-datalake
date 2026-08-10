package org.radiant.opendatalake.normalized.gnomad

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.array_contains
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime


// Here "v1" represents the opendatalake contract version (1.x.x).
// The raw dataset version compatible with this normalizer is 4.1.
case class GnomadCNV_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String)
  extends ContractETLP(rc, sourceDatasetId = "normalized_gnomad_cnv", tablePrefix, major = 1) {

  val gnomad_vcf: DatasetConf = conf.getDataset("raw_gnomad_cnv")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(gnomad_vcf.id -> RawInput.readVersioned(gnomad_vcf.id, version, rawStorage))
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    // - Glow reads the CNV end from the INFO END field into its own `end` column, leaving INFO_END null.
    // - qual is not selected: the QUAL column is `.` on every record of the release.
    // - sc/sn/sf are sample counts, not allele counts: a CNV VCF carries no AC/AF/AN
    // - gnomAD declares sc and sn as Float although they count individuals, hence the cast.
    // - FILTER holds PASS or FAIL only, so the FAIL records are dropped and the column is not published.
    data(gnomad_vcf.id)
      .filter(array_contains($"filters", "PASS"))
      .select(
        chromosome,
        start,
        end,
        reference,
        alternate,
        name,
        $"INFO_SVTYPE" as "svtype",
        $"INFO_SVLEN" as "svlen",
        $"INFO_SC".cast("long") as "sc",
        $"INFO_SN".cast("long") as "sn",
        $"INFO_SF" as "sf"
      )
  }

}
