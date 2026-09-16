package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.models.normalized.NormalizedClinvar
import bio.ferlab.datalake.testutils.models.raw.RawClinvar
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}


class ClinvarV1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_clinvar")

  val destination: DatasetConf =
    new Clinvar_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "clinvar").mainDestination

  assert(destination.table.map(_.name).contains("clinvar_v1"), s"MAJOR 1 must publish to clinvar_v1, not ${destination.table}")
  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "transform" should "transform ClinvarInput to ClinvarOutput" in {
    val inputData = Map(source.id -> Seq(RawClinvar("2"), RawClinvar("3")).toDF())

    val resultDF = new Clinvar_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "clinvar").transformSingle(inputData)

    val expectedResults = Seq(NormalizedClinvar("2"), NormalizedClinvar("3"))

    resultDF.as[NormalizedClinvar].collect() should contain allElementsOf expectedResults
  }

  it should "publish both locus and locus_hash (SJRA-1811 -- Radiant's StarRocks clinvar target stores both)" in {
    val inputData = Map(source.id -> Seq(RawClinvar("2")).toDF())

    val row = new Clinvar_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "clinvar")
      .transformSingle(inputData)
      .select("locus", "locus_hash")
      .head()

    // RawClinvar carries Glow's 0-based start 69359260, so the published POS is 69359261.
    row.getString(0) shouldBe "2-69359261-T-A"
    // sha256("2-69359261-T-A")
    row.getString(1) shouldBe "f2773c750417c2d1ccd6d0ab10a935575d4060c52e78d3e33bf7d68b74264379"
  }

  /*
    Since SJRA-1546 §2.1, loadSingle publishes through WapLoader: the rows land on a branch named after the
    dataset_version and `main` is left permanently empty, so `destination.read` (which resolves to the
    table's default ref) is no longer the way to see what was written.
  */
  private val tableName: String = destination.table.map(_.fullName).get

  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName)

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(NormalizedClinvar("1", name = "first"), NormalizedClinvar("2"))
    val secondLoad = Seq(NormalizedClinvar("1", name = "second"), NormalizedClinvar("3"))

    val job = new Clinvar_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "clinvar")

    job.loadSingle(firstLoad.toDF())
    onBranch("test").as[NormalizedClinvar].collect() should contain allElementsOf firstLoad

    // Re-importing the same dataset_version replaces the branch rather than merging into it (§3.4).
    job.loadSingle(secondLoad.toDF())
    onBranch("test").select("chromosome", "start", "end", "reference", "alternate", "name")
    onBranch("test").as[NormalizedClinvar].collect() should contain theSameElementsAs secondLoad

    withClue("main must stay empty — consumers read the dataset_version branch: ") {
      spark.table(tableName).count() shouldBe 0
    }

    val refs = spark.sql(s"SELECT name FROM $tableName.refs").collect().map(_.getString(0)).toSet
    refs should contain allOf ("main", "test")
    withClue(s"the transient audit branch outlived the import, refs were $refs: ") {
      refs.filter(_.startsWith("audit")) shouldBe empty
    }
  }
}
