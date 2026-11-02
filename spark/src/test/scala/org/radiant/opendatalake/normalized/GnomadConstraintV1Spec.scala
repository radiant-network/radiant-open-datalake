package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.radiant.opendatalake.normalized.gnomad.GnomadConstraint_v1
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

/*
  The fixture is five verbatim lines of gnomad.v4.1.2.constraint_metrics.tsv.bgz (header + 4 rows), gzipped:
  - A1BG NM_130786.4     RefSeq, no locus — must be dropped
  - A1BG ENST00000263100 canonical MANE Select, every metric set
  - A1BG ENST00000600966 non-canonical
  - AAMDC ENST00000529666 gene flag low_exome_coverage, constraint flag no_exp_lof: lof metrics are NA and the
    lof gen_anc arrays hold only nulls
  It sits at the versioned raw path so `extract` is exercised as on EMR.
 */
class GnomadConstraintV1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  private val version = "4.1.2"
  private val rawStorage: String = getClass.getResource("/raw/landing").getPath

  private def job = GnomadConstraint_v1(TestETLContext(), version, rawStorage, tablePrefix = "gnomad_constraint")

  private val destination: DatasetConf = job.mainDestination
  assert(
    destination.table.map(_.name).contains("gnomad_constraint_v1"),
    s"MAJOR 1 must publish to gnomad_constraint_v1, not ${destination.table}"
  )

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  private def transformed: DataFrame = job.transformSingle(job.extract())

  private def byTranscript(df: DataFrame): Map[String, Row] =
    df.collect().map(r => r.getAs[String]("transcript") -> r).toMap

  "transform" should "keep only Ensembl transcripts" in {
    byTranscript(transformed).keySet shouldBe Set("ENST00000263100", "ENST00000600966", "ENST00000529666")
  }

  it should "publish every source column with dots replaced by underscores" in {
    val names = transformed.columns
    names.filter(_.contains('.')) shouldBe empty
    names.length shouldBe 113
    names should contain allOf ("symbol", "start", "end", "lof_pLI", "lof_oe_ci_upper", "lof_gen_anc_obs_global",
      "gene_quality_metrics_exome_prop_bp_AN90")
    names should contain noneOf ("gene", "start_position", "end_position")
  }

  it should "type the columns" in {
    val schema = transformed.schema
    schema("start").dataType shouldBe LongType
    schema("canonical").dataType shouldBe BooleanType
    schema("lof_obs").dataType shouldBe IntegerType
    schema("lof_possible").dataType shouldBe LongType
    schema("lof_oe_ci_upper").dataType shouldBe FloatType
    schema("lof_pNull").dataType shouldBe DoubleType
    schema("gene_flags").dataType shouldBe ArrayType(StringType)
    schema("lof_gen_anc_obs_global").dataType shouldBe ArrayType(IntegerType)
    schema("lof_gen_anc_exp_afr").dataType shouldBe ArrayType(DoubleType)
  }

  it should "normalize a fully populated MANE Select row" in {
    val a1bg = byTranscript(transformed)("ENST00000263100")

    a1bg.getAs[String]("chromosome") shouldBe "19"
    a1bg.getAs[Long]("start") shouldBe 58345183L
    a1bg.getAs[Long]("end") shouldBe 58353492L
    a1bg.getAs[String]("symbol") shouldBe "A1BG"
    a1bg.getAs[String]("gene_id") shouldBe "ENSG00000121410"
    a1bg.getAs[String]("transcript_version") shouldBe "ENST00000263100.8"
    a1bg.getAs[Int]("transcript_level") shouldBe 2
    a1bg.getAs[Boolean]("canonical") shouldBe true
    a1bg.getAs[Boolean]("mane_select") shouldBe true
    a1bg.getAs[Seq[String]]("gene_flags") shouldBe empty
    a1bg.getAs[Seq[String]]("constraint_flags") shouldBe empty

    a1bg.getAs[Int]("lof_obs") shouldBe 44
    a1bg.getAs[Long]("lof_possible") shouldBe 214L
    a1bg.getAs[Float]("lof_pLI") shouldBe 6.7352e-15f
    a1bg.getAs[Float]("lof_oe_ci_upper") shouldBe 1.2292f
    a1bg.getAs[Int]("lof_oe_ci_upper_rank") shouldBe 14080
    a1bg.getAs[Int]("lof_oe_ci_upper_bin_decile") shouldBe 7
    a1bg.getAs[Int]("lof_oe_ci_upper_bin_sextile") shouldBe 4
    a1bg.getAs[Double]("lof_pNull") shouldBe 0.61708
    a1bg.getAs[Float]("mis_z_score") shouldBe -0.29516f

    a1bg.getAs[Seq[Int]]("lof_gen_anc_obs_global") shouldBe
      Seq(0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 4, 4, 4, 5, 6, 7, 9, 12, 22, 39, 42)
    a1bg.getAs[Seq[Int]]("lof_gen_anc_obs_afr") shouldBe Seq(0, 0, 0, 0, 0, 0, 0, 2, 2, 3)
  }

  it should "read NA as null and keep the flags of an unconstrained row" in {
    val aamdc = byTranscript(transformed)("ENST00000529666")

    aamdc.getAs[Seq[String]]("gene_flags") shouldBe Seq("low_exome_coverage")
    aamdc.getAs[Seq[String]]("constraint_flags") shouldBe Seq("no_exp_lof")
    aamdc.getAs[Long]("lof_possible") shouldBe 0L
    Seq("lof_obs", "lof_exp", "lof_pLI", "lof_oe_ci_upper", "lof_oe_ci_upper_rank", "lof_pNull").foreach { c =>
      withClue(s"$c: ") { aamdc.isNullAt(aamdc.fieldIndex(c)) shouldBe true }
    }

    val nullsOnly = aamdc.getAs[Seq[Any]]("lof_gen_anc_obs_global")
    nullsOnly should have length 21
    nullsOnly.forall(_ == null) shouldBe true
  }

  private val tableName: String = destination.table.map(_.fullName).get

  "load" should "publish the version to its own branch and leave main empty" in {
    job.loadSingle(transformed)

    spark.read.option("branch", version).table(tableName).count() shouldBe 3

    withClue("main must stay empty — consumers read the dataset_version branch: ") {
      spark.table(tableName).count() shouldBe 0
    }
  }
}
