package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.models.normalized.NormalizedClinvar
import bio.ferlab.datalake.testutils.models.raw.RawClinvar
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}


class ClinvarV1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_clinvar")
  val destination: DatasetConf = conf.getDataset("normalized_clinvar")

  assert(destination.table.isDefined, "table normalized_clinvar dataset (destination) must be defined in test config")
  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "transform" should "transform ClinvarInput to ClinvarOutput" in {
    val inputData = Map(source.id -> Seq(RawClinvar("2"), RawClinvar("3")).toDF())

    val resultDF = new Clinvar_v1(TestETLContext(), version = "test", rawStorage = "").transformSingle(inputData)

    val expectedResults = Seq(NormalizedClinvar("2"), NormalizedClinvar("3"))

    resultDF.as[NormalizedClinvar].collect() should contain allElementsOf expectedResults
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

    val job = new Clinvar_v1(TestETLContext(), version = "test", rawStorage = "")

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
