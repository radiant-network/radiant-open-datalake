package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import bio.ferlab.datalake.testutils.models.enriched.EnrichedSpliceAi
import bio.ferlab.datalake.testutils.models.raw.RawSpliceAi
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}


class SpliceAiV1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_spliceai")

  private def job = new SpliceAi_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "spliceai")

  val destination: DatasetConf = job.mainDestination

  assert(destination.table.map(_.name).contains("spliceai_v1"), s"MAJOR 1 must publish to spliceai_v1, not ${destination.table}")
  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "transformSingle" should "normalize the raw SpliceAI scores and append max_score (EnrichedSpliceAi)" in {
    val inputData = Map(source.id -> Seq(RawSpliceAi("2"), RawSpliceAi("3")).toDF())

    val resultDF = job.transformSingle(inputData)

    val expectedResults = Seq(EnrichedSpliceAi("2"), EnrichedSpliceAi("3"))
    resultDF.as[EnrichedSpliceAi].collect() should contain allElementsOf expectedResults
  }

  /*
    Since SJRA-1546 §2.1, loadSingle publishes through WapLoader: the rows land on a branch named after the
    dataset_version and `main` is left permanently empty, so `destination.read` (which resolves to the
    table's default ref) is no longer the way to see what was written.
  */
  private val tableName: String = destination.table.map(_.fullName).get

  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName)

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(EnrichedSpliceAi("1"), EnrichedSpliceAi("2"))
    val secondLoad = Seq(EnrichedSpliceAi("2"), EnrichedSpliceAi("3"))

    job.loadSingle(firstLoad.toDF())
    onBranch("test").as[EnrichedSpliceAi].collect() should contain allElementsOf firstLoad

    // Re-importing the same dataset_version replaces the branch rather than merging into it (§3.4).
    job.loadSingle(secondLoad.toDF())
    onBranch("test").as[EnrichedSpliceAi].collect() should contain theSameElementsAs secondLoad

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
