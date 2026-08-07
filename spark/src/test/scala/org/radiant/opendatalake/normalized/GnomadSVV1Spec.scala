package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.apache.spark.sql.Row
import org.radiant.opendatalake.normalized.gnomad.GnomadSV_v1
import org.radiant.opendatalake.testutils.SparkSpec

/*
  The fixture is the real header of gnomad.v4.1.sv.sites.vcf.gz (619 INFO fields) plus five
  records taken verbatim from the release, one per scenario:
    chr1:10000  <DUP> FILTER=HIGH_NCR             -> dropped, did not pass
    chr1:10434  <BND> FILTER=HIGH_NCR;UNRESOLVED  -> dropped, and multi-valued FILTER
    chr1:11000  <DUP> FILTER=PASS                 -> published
    chr1:40000  <DEL> FILTER=PASS                 -> published
    chr1:54771  <INS> FILTER=PASS                 -> published, proves we do not filter on svtype
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
    rows should have length 3

    // chr1:11000 <DUP>
    val dup = rows(0)
    dup.getAs[String]("chromosome") shouldBe "1"
    dup.getAs[Long]("start") shouldBe 11000L
    // INFO END is 51000. The GenomicImplicits helpers add 1 to both of the VCF reader's 0-based
    // bounds, so the published interval is [start, end) and `end` is one past END.
    dup.getAs[Long]("end") shouldBe 51001L
    dup.getAs[String]("reference") shouldBe "N"
    dup.getAs[String]("alternate") shouldBe "<DUP>"
    dup.getAs[String]("name") shouldBe "gnomAD-SV_v3_DUP_chr1_7d73682f"
    dup.getAs[String]("svtype") shouldBe "DUP"
    dup.getAs[Long]("ac") shouldBe 7119L
    dup.getAs[Long]("an") shouldBe 106172L
    dup.getAs[Double]("af") shouldBe 0.067052 +- 1e-9
    dup.getAs[Long]("n_het") shouldBe 4595L
    dup.getAs[Long]("n_homalt") shouldBe 1262L
    dup.getAs[Long]("n_bi_genos") shouldBe 53086L

    // chr1:40000 <DEL>
    val del = rows(1)
    del.getAs[Long]("start") shouldBe 40000L
    del.getAs[Long]("end") shouldBe 47001L
    del.getAs[String]("name") shouldBe "gnomAD-SV_v3_DEL_chr1_b26f63f7"
    del.getAs[String]("svtype") shouldBe "DEL"
    del.getAs[Long]("ac") shouldBe 12574L
    del.getAs[Long]("an") shouldBe 126092L
    del.getAs[Double]("af") shouldBe 0.099721 +- 1e-9
    del.getAs[Long]("n_het") shouldBe 12564L
    del.getAs[Long]("n_homalt") shouldBe 5L
    del.getAs[Long]("n_bi_genos") shouldBe 63046L
  }

  it should "drop records that did not pass the source filters" in {
    val names = transformed.map(_.getAs[String]("name"))

    // A DUP carrying a single filter: the rejection is on FILTER, not on svtype.
    names should not contain "gnomAD-SV_v3_DUP_chr1_01c2781c"

    // FILTER=HIGH_NCR;UNRESOLVED — the source field is multi-valued, which is why the normalizer
    // tests it with array_contains rather than an equality.
    names should not contain "gnomAD-SV_v3_BND_chr1_1a45f73a"
  }

  it should "keep PASS records whose svtype is neither DEL nor DUP" in {
    // radiant restricts to DUP/DEL in its own SQL; the contract publishes every type.
    val ins = transformed.find(_.getAs[String]("svtype") == "INS")

    ins.map(_.getAs[String]("name")) shouldBe Some("gnomAD-SV_v3_INS_chr1_be9a7ac8")
    ins.map(_.getAs[Long]("ac")) shouldBe Some(1L)
  }

  it should "publish exactly the thirteen columns of the contract" in {
    transformed.head.schema.fieldNames should contain theSameElementsInOrderAs Seq(
      "chromosome", "start", "end", "reference", "alternate", "name",
      "svtype", "ac", "an", "af", "n_het", "n_homalt", "n_bi_genos"
    )
  }
}
