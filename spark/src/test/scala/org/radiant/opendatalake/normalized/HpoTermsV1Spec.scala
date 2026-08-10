package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

class HpoTermsV1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  private val source: DatasetConf = conf.getDataset("raw_hpo_terms")

  private def job = new HpoTerms_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "hpo_terms")

  private val destination: DatasetConf = job.mainDestination
  assert(
    destination.table.map(_.name).contains("hpo_terms_v1"),
    s"MAJOR 1 must publish to hpo_terms_v1, not ${destination.table}"
  )

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  private val obo =
    """format-version: 1.2
      |
      |[Term]
      |id: HP:0000001
      |name: All
      |alt_id: HP:0000000
      |
      |[Term]
      |id: HP:0000118
      |name: Phenotypic abnormality
      |is_a: HP:0000001 ! All
      |
      |[Typedef]
      |id: part_of
      |name: part of
      |""".stripMargin

  "transform" should "parse the .obo via obo-parser into flat HPO terms, skipping non-Term stanzas" in {
    val inputData = Map(source.id -> Seq(obo.getBytes("UTF-8")).toDF("content"))

    val result = job.transformSingle(inputData).as[HpoTerm].collect()

    result should contain theSameElementsAs Seq(
      HpoTerm("HP:0000001", "All", Nil, Seq("HP:0000000")),
      HpoTerm("HP:0000118", "Phenotypic abnormality", Seq("HP:0000001"), Nil)
    )
  }

  it should "refuse to publish when nothing parses" in {
    val cases = Seq(
      ("not OBO at all", Seq("this is not an ontology".getBytes("UTF-8")), "No HPO term parsed"),
      ("OBO with no [Term] stanza", Seq("format-version: 1.2\n".getBytes("UTF-8")), "No HPO term parsed"),
      ("no file matched the raw glob", Seq.empty[Array[Byte]], "found 0"),
      ("two files matched the raw glob", Seq(obo.getBytes("UTF-8"), obo.getBytes("UTF-8")), "found 2")
    )

    cases.foreach { case (clue, content, expected) =>
      withClue(s"$clue: ") {
        val ex = the[IllegalArgumentException] thrownBy
          job.transformSingle(Map(source.id -> content.toDF("content")))

        ex.getMessage should include(expected)
      }
    }
  }

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName).as[HpoTerm]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(HpoTerm("HP:1", "first", Nil, Nil))
    val secondLoad = Seq(
      HpoTerm("HP:1", "second", Nil, Nil),
      HpoTerm("HP:2", "other", Seq("HP:1"), Seq("HP:0"))
    )

    job.loadSingle(firstLoad.toDF())
    onBranch("test").collect() should contain theSameElementsAs firstLoad

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
