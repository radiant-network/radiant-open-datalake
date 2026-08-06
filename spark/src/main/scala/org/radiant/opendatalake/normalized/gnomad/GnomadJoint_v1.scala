package org.radiant.opendatalake.normalized.gnomad

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.DataFrame
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime


// Here "v1" represents the opendatalake contract version (1.x.x).
// The raw dataset version compatible with this normalizer is 4.1.
case class GnomadJoint_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String)
  extends ContractETLP(rc, sourceDatasetId = "normalized_gnomad_joint", tablePrefix, major = 1) {


  val gnomad_vcf: DatasetConf = conf.getDataset("raw_gnomad_joint")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] = {

    Map(gnomad_vcf.id -> RawInput.readVersioned(gnomad_vcf.id, version, rawStorage))
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    import spark.implicits._

    val df = data(gnomad_vcf.id)

    // qual and name are not selected because they are always null in gnomAD sites VCFs.
    val intermediate = df
      .select(
        chromosome +:
        start +:
        end +:
        reference +:
        alternate +:
        flattenInfo(df): _*
      )

    intermediate.select(
      $"chromosome",
      $"start",
      $"end",
      $"reference",
      $"alternate",
      $"ac_joint".cast("long"),
      $"af_joint",
      $"an_joint".cast("long"),
      $"nhomalt_joint".cast("long") as "hom_joint",
      $"ac_genomes".cast("long"),
      $"af_genomes",
      $"an_genomes".cast("long"),
      $"nhomalt_genomes".cast("long") as "hom_genomes",
      $"ac_exomes".cast("long"),
      $"af_exomes",
      $"an_exomes".cast("long"),
      $"nhomalt_exomes".cast("long") as "hom_exomes",
    )
  }

  override val defaultRepartition: DataFrame => DataFrame = RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1000))

}
