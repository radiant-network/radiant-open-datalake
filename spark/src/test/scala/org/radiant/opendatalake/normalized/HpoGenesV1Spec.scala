package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

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

  // Mirrors the genes_to_phenotype.txt header the CSV reader produces (all string columns, `-` already nulled).
  private def rawRow(ncbiGeneId: String, symbol: String, hpoId: String, hpoName: String,
                     frequency: String, diseaseId: String) =
    (ncbiGeneId, symbol, hpoId, hpoName, frequency, diseaseId)

  private val raw = Seq(
    rawRow("10", "NAT2", "HP:0000007", "Autosomal recessive inheritance", null, "OMIM:243400"),
    rawRow("10", "NAT2", "HP:0001939", "Abnormality of metabolism/homeostasis", "1/2", "OMIM:243400")
  ).toDF("ncbi_gene_id", "gene_symbol", "hpo_id", "hpo_name", "frequency", "disease_id")

  "transform" should "rename to the enriched.Genes schema, cast the gene id, and null empty frequency" in {
    val result = job.transformSingle(Map(source.id -> raw)).as[HpoGene].collect()

    result should contain theSameElementsAs Seq(
      HpoGene(10L, "NAT2", "HP:0000007", "Autosomal recessive inheritance", None, "OMIM:243400"),
      HpoGene(10L, "NAT2", "HP:0001939", "Abnormality of metabolism/homeostasis", Some("1/2"), "OMIM:243400")
    )
  }

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName).as[HpoGene]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(HpoGene(1L, "A", "HP:1", "first", None, "OMIM:1"))
    val secondLoad = Seq(
      HpoGene(1L, "A", "HP:1", "first", Some("2/2"), "OMIM:1"),
      HpoGene(2L, "B", "HP:2", "second", None, "OMIM:2")
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
