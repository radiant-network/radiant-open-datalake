package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import bio.ferlab.datalake.testutils.models.normalized.NormalizedTopmed
import bio.ferlab.datalake.testutils.models.raw.{RawTopMedFreeze10, RawTopMedFreeze8}
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

class TopMedSpec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_topmed_bravo")

  private def job = TopMed_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "topmed_bravo")

  val destination: DatasetConf = job.mainDestination

  assert(destination.table.map(_.name).contains("topmed_bravo_v1"), s"MAJOR 1 must publish to topmed_bravo_v1, not ${destination.table}")
  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "transform freeze_8" should "transform TOPMed freeze_8 input to TOPMed output" in {
    val df = Seq(RawTopMedFreeze8()).toDF()

    val result = job.transformSingle(Map(source.id -> df))

    result.as[NormalizedTopmed].collect() should contain theSameElementsAs Seq(NormalizedTopmed())
  }

  "transform freeze_10" should "transform TOPMed freeze_10 input to TOPMed output (recovering AN from AC/AF)" in {
    val df = Seq(RawTopMedFreeze10()).toDF()

    val result = job.transformSingle(Map(source.id -> df))

    result.as[NormalizedTopmed].collect() should contain theSameElementsAs Seq(NormalizedTopmed(name = None))
  }

  private val tableName: String = destination.table.map(_.fullName).get

  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName)

  "load" should "publish the version to its own branch and leave main empty" in {
    val rows = Seq(NormalizedTopmed(), NormalizedTopmed(name = None))

    job.loadSingle(rows.toDF())

    onBranch("test").count() shouldBe rows.size
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
