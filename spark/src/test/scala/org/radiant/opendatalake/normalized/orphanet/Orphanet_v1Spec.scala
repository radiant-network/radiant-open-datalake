package org.radiant.opendatalake.normalized.orphanet

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import bio.ferlab.datalake.testutils.models.normalized.NormalizedOrphanetGeneSet
import bio.ferlab.datalake.testutils.models.raw.{RawOrphanetProduct6, RawOrphanetProduct9}
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

class Orphanet_v1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  private val geneAssociation: DatasetConf = conf.getDataset("raw_orphanet_gene_association")
  private val diseaseHistory: DatasetConf = conf.getDataset("raw_orphanet_disease_history")

  private def job = Orphanet_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "orphanet")

  private val destination: DatasetConf = job.mainDestination
  assert(
    destination.table.map(_.name).contains("orphanet_v1"),
    s"MAJOR 1 must publish to orphanet_v1, not ${destination.table}"
  )

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "transform" should "left-join gene associations (product6) with disorder ages (product9) on orpha_code" in {
    // product9 shares product6's orpha_code so the two rows join.
    val product6 = Seq(RawOrphanetProduct6()).toDF()
    val product9 = Seq(RawOrphanetProduct9(orpha_code = 447)).toDF()

    val result = job.transformSingle(Map(geneAssociation.id -> product6, diseaseHistory.id -> product9))

    val expected = NormalizedOrphanetGeneSet(
      orpha_code = 447,
      disorder_id = 21,
      expert_link = "http://www.orpha.net/consor/cgi-bin/OC_Exp.php?lng=en&Expert=447",
      name = "Paroxysmal nocturnal hemoglobinuria",
      disorder_type_id = 21394,
      disorder_type_name = "Disease",
      disorder_group_id = 36547,
      disorder_group_name = "Disorder",
      gene_source_of_validation = "22305531[PMID]",
      gene_id = 19197,
      gene_symbol = "PIGA",
      gene_name = "phosphatidylinositol glycan anchor biosynthesis class A",
      gene_synonym_list = List(
        "GPI3",
        "paroxysmal nocturnal hemoglobinuria",
        "phosphatidylinositol N-acetylglucosaminyltransferase"
      ),
      ensembl_gene_id = "ENSG00000165195",
      genatlas_gene_id = "PIGA",
      HGNC_gene_id = "8957",
      omim_gene_id = "311770",
      reactome_gene_id = "P37287",
      swiss_prot_gene_id = "P37287",
      association_type = "Disease-causing somatic mutation(s) in",
      association_type_id = 17955,
      association_status = "Assessed",
      gene_locus_id = 22873,
      gene_locus = "Xp22.2",
      gene_locus_key = 1,
      average_age_of_onset = List("All ages"),
      average_age_of_death = List("any age"),
      type_of_inheritance = List("Autosomal dominant")
    )

    result.as[NormalizedOrphanetGeneSet].collect() should contain theSameElementsAs Seq(expected)
  }

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) =
    spark.read.option("branch", branch).table(tableName).as[NormalizedOrphanetGeneSet]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(NormalizedOrphanetGeneSet(gene_symbol = "GENE1"))
    val secondLoad = Seq(
      NormalizedOrphanetGeneSet(gene_symbol = "GENE1"),
      NormalizedOrphanetGeneSet(gene_symbol = "GENE2")
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
