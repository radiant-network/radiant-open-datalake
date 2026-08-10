package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

case class HpoGeneRow(ncbi_gene_id: String,
                      gene_symbol: String,
                      hpo_id: String,
                      hpo_name: String,
                      frequency: Option[String],
                      disease_id: String)

class HpoGenesV1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  private val source: DatasetConf = conf.getDataset("raw_hpo_genes")

  private def job = new HpoGenes_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "hpo_genes")

  private val destination: DatasetConf = job.mainDestination
  assert(
    destination.table.map(_.name).contains("hpo_genes_v1"),
    s"MAJOR 1 must publish to hpo_genes_v1, not ${destination.table}"
  )

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  private val rows = Seq(
    HpoGeneRow("10", "NAT2", "HP:0000007", "Autosomal recessive inheritance", None, "OMIM:243400"),
    HpoGeneRow("10", "NAT2", "HP:0001939", "Abnormality of metabolism/homeostasis", Some("1/2"), "OMIM:243400")
  )

  "transform" should "pass the source columns through unchanged (faithful to source)" in {
    val out = job.transformSingle(Map(source.id -> rows.toDF()))

    out.columns should contain theSameElementsInOrderAs
      Seq("ncbi_gene_id", "gene_symbol", "hpo_id", "hpo_name", "frequency", "disease_id")
    out.as[HpoGeneRow].collect() should contain theSameElementsAs rows
  }

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName).as[HpoGeneRow]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(HpoGeneRow("1", "A", "HP:1", "first", None, "OMIM:1"))
    val secondLoad = Seq(
      HpoGeneRow("1", "A", "HP:1", "first", Some("2/2"), "OMIM:1"),
      HpoGeneRow("2", "B", "HP:2", "second", None, "OMIM:2")
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
