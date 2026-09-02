package org.radiant.opendatalake.normalized.omim

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import bio.ferlab.datalake.testutils.models.normalized.{NormalizedOmimGeneSet, PHENOTYPE}
import bio.ferlab.datalake.testutils.models.raw.RawOmimGeneSet
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

class Omim_v1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  private val source: DatasetConf = conf.getDataset("raw_omim_gene_set")

  private def job = Omim_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "omim")

  private val destination: DatasetConf = job.mainDestination
  assert(destination.table.map(_.name).contains("omim_v1"), s"MAJOR 1 must publish to omim_v1, not ${destination.table}")

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "transform" should "transform RawOmimGeneSet to NormalizedOmimGeneSet" in {
    val rawOmimGeneSet2PhenotypeName = "Acute myeloid leukemia, somatic"
    val rawOmimGeneSet3PhenotypeName = "Hemolytic anemia due to phosphofructokinase deficiency"

    val inputData = Map(source.id -> Seq(
      RawOmimGeneSet(), // phenotype with omim_id and inheritance
      RawOmimGeneSet(_c12 = rawOmimGeneSet2PhenotypeName + ", 601626 (3)"), // omim_id, no inheritance
      RawOmimGeneSet(_c12 = rawOmimGeneSet3PhenotypeName + " (1)") // no omim_id, no inheritance
    ).toDF())

    val resultDF = job.transformSingle(inputData)

    val expectedResults = Seq(
      NormalizedOmimGeneSet(),
      NormalizedOmimGeneSet(phenotype = PHENOTYPE(name = rawOmimGeneSet2PhenotypeName, omim_id = "601626", null, null)),
      NormalizedOmimGeneSet(phenotype = PHENOTYPE(name = rawOmimGeneSet3PhenotypeName, omim_id = null, null, null))
    )
    resultDF.as[NormalizedOmimGeneSet].collect() shouldBe expectedResults
  }

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName).as[NormalizedOmimGeneSet]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(NormalizedOmimGeneSet(omim_gene_id = 100))
    val secondLoad = Seq(
      NormalizedOmimGeneSet(omim_gene_id = 100),
      NormalizedOmimGeneSet(omim_gene_id = 200)
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
