package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import bio.ferlab.datalake.testutils.models.normalized.NormalizedDddGeneCensus
import bio.ferlab.datalake.testutils.models.raw.RawDDDGeneSet
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

class DDD_v1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  private val source: DatasetConf = conf.getDataset("raw_ddd_gene_set")

  private def job = DDD_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "ddd")

  private val destination: DatasetConf = job.mainDestination
  assert(destination.table.map(_.name).contains("ddd_v1"), s"MAJOR 1 must publish to ddd_v1, not ${destination.table}")

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "transform" should "transform DDD Gene input to DDD Gene output" in {
    val df = Seq(RawDDDGeneSet()).toDF()

    val result = job.transformSingle(Map(source.id -> df))

    result.as[NormalizedDddGeneCensus].collect() should contain theSameElementsAs Seq(NormalizedDddGeneCensus())
  }

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName).as[NormalizedDddGeneCensus]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(NormalizedDddGeneCensus(symbol = "GENE1"))
    val secondLoad = Seq(
      NormalizedDddGeneCensus(symbol = "GENE1"),
      NormalizedDddGeneCensus(symbol = "GENE2")
    )

    job.loadSingle(firstLoad.toDF())
    onBranch("test").collect() should contain theSameElementsAs firstLoad

    // Re-importing the same dataset_version replaces the branch rather than merging into it (§3.4).
    job.loadSingle(secondLoad.toDF())
    onBranch("test").collect() should contain theSameElementsAs secondLoad

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
