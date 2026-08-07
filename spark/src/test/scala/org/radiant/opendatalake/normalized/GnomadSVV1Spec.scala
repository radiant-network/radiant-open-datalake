package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.apache.spark.sql.Row
import org.radiant.opendatalake.normalized.gnomad.GnomadSV_v1
import org.radiant.opendatalake.testutils.SparkSpec

/*
  Use sample vcf file constructed consisting in the header + rows from the original sites file
 */
class GnomadSVV1Spec extends SparkSpec {

  private val source: DatasetConf = conf.getDataset("raw_gnomad_sv")

  private def job =
    GnomadSV_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "gnomad_sv")

  assert(
    job.mainDestination.table.map(_.name).contains("gnomad_sv_v1"),
    s"MAJOR 1 must publish to gnomad_sv_v1, not ${job.mainDestination.table}"
  )

  private def transformed: Array[Row] = {
    val fixture = getClass.getResource("/input_vcf/gnomadV4SV.vcf").getPath
    val raw = spark.read.format("vcf").option("flattenInfoFields", "true").load(fixture)
    job.transformSingle(Map(source.id -> raw)).orderBy("start").collect()
  }

  "transformSingle" should "publish the locus, the SV type and the global frequencies of each record" in {
    val rows = transformed
    rows should have length 2

    // chr22:10510033 <DEL>
    val del = rows(0)
    del.getAs[String]("chromosome") shouldBe "22"
    del.getAs[Long]("start") shouldBe 10510033L
    // POS in the VCF, but INFO END + 1: the GenomicImplicits helpers add 1 to both of Glow's
    // 0-based bounds, so the published interval is [start, end) and `end` is one past END=10520033.
    del.getAs[Long]("end") shouldBe 10520034L
    del.getAs[String]("reference") shouldBe "N"
    del.getAs[String]("alternate") shouldBe "<DEL>"
    del.getAs[String]("name") shouldBe "gnomAD-SV_v3_DEL_chr22_1a2b3c4d"
    del.getAs[String]("svtype") shouldBe "DEL"
    del.getAs[Long]("ac") shouldBe 1262L
    del.getAs[Long]("an") shouldBe 126092L
    del.getAs[Double]("af") shouldBe 0.010008 +- 1e-9
    del.getAs[Long]("n_het") shouldBe 1200L
    del.getAs[Long]("n_homalt") shouldBe 31L
    del.getAs[Long]("n_bi_genos") shouldBe 63046L

    // chr22:10736031 <DUP>
    val dup = rows(1)
    dup.getAs[String]("svtype") shouldBe "DUP"
    dup.getAs[Long]("n_homalt") shouldBe 0L
  }

  it should "drop records that did not pass the source filters" in {
    // The fixture holds a third record, FILTER=UNRESOLVED, which must not reach the table.
    transformed.map(_.getAs[String]("name")) should not contain "gnomAD-SV_v3_CPX_chr22_da6f1b5c"
  }

  it should "publish exactly the thirteen columns of the contract" in {
    transformed.head.schema.fieldNames should contain theSameElementsInOrderAs Seq(
      "chromosome", "start", "end", "reference", "alternate", "name",
      "svtype", "ac", "an", "af", "n_het", "n_homalt", "n_bi_genos"
    )
  }
}
