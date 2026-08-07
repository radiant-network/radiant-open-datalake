package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.apache.spark.sql.Row
import org.radiant.opendatalake.normalized.gnomad.GnomadCNV_v1
import org.radiant.opendatalake.testutils.SparkSpec

/*
  Use sample vcf file constructed consisting in the header + rows from the original exome CNV file
 */
class GnomadCNVV1Spec extends SparkSpec {

  private val source: DatasetConf = conf.getDataset("raw_gnomad_cnv")

  private def job =
    GnomadCNV_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "gnomad_cnv")

  assert(
    job.mainDestination.table.map(_.name).contains("gnomad_cnv_v1"),
    s"MAJOR 1 must publish to gnomad_cnv_v1, not ${job.mainDestination.table}"
  )

  "transformSingle" should "publish the locus, the CNV type and the global sample counts of each record" in {
    val fixture = getClass.getResource("/input_vcf/gnomadV4CNV.vcf").getPath
    val raw = spark.read.format("vcf").option("flattenInfoFields", "true").load(fixture)

    val rows: Array[Row] = job.transformSingle(Map(source.id -> raw)).collect()
    rows should have length 1

    // Should match: chr1:925634-931187 DEL, 3 carriers out of 464277 releasable samples
    val del = rows.head
    del.getAs[String]("chromosome") shouldBe "1"
    del.getAs[Long]("start") shouldBe 925634L
    del.getAs[Long]("end") shouldBe 931188L
    del.getAs[String]("reference") shouldBe "N"
    del.getAs[String]("alternate") shouldBe "<DEL>"
    del.getAs[String]("name") shouldBe "variant_is_80_2__DEL"
    del.getAs[Seq[String]]("filters") shouldBe Seq("PASS")
    del.getAs[String]("svtype") shouldBe "DEL"
    del.getAs[Int]("svlen") shouldBe 5553
    del.getAs[Float]("sc") shouldBe 3f
    del.getAs[Float]("sn") shouldBe 464277f
    del.getAs[Float]("sf") shouldBe 6.46165974192131e-06f
  }

  it should "publish nothing beyond the locus, the CNV type and the global counts" in {
    val fixture = getClass.getResource("/input_vcf/gnomadV4CNV.vcf").getPath
    val raw = spark.read.format("vcf").option("flattenInfoFields", "true").load(fixture)

    // The release carries about 112 INFO fields; publishing one commits the contract to it, since
    // dropping a column later is a MAJOR while adding one is a MINOR carried by schema evolution.
    val fields = job.transformSingle(Map(source.id -> raw)).schema.fieldNames
    fields should have length 12
    fields.filter(f => f.contains("_xx") || f.contains("_xy") || f.contains("_nfe")) shouldBe empty
    fields should not contain "qual"

    // Glow reads the INFO END field into its own `end`, so a flattened INFO would yield a second one.
    fields.count(_ == "end") shouldBe 1
  }
}
