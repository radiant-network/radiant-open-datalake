package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import bio.ferlab.datalake.testutils.models.enriched.{EnrichedSpliceAi, MAX_SCORE}
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

  // Format of one SpliceAI INFO entry: allele|symbol|ds_ag|ds_al|ds_dg|ds_dl|dp_ag|dp_al|dp_dg|dp_dl
  private def spliceaiEntry(symbol: String, dsAg: Double, dsAl: Double, dsDg: Double, dsDl: Double): String =
    s"C|$symbol|$dsAg|$dsAl|$dsDg|$dsDl|0|0|0|0"

  private val aboveCutoff: Seq[String] = Seq("G|KCNH1|0.3|0.1|0.1|0.1|-3|36|32|22")

  private def enrichedAboveCutoff(chromosome: String): EnrichedSpliceAi =
    EnrichedSpliceAi(chromosome, ds_ag = 0.3, max_score = MAX_SCORE(0.3, Some(Seq("AG"))))

  "transformSingle" should "normalize the raw SpliceAI scores and append max_score (EnrichedSpliceAi)" in {
    val inputData = Map(source.id -> Seq(
      RawSpliceAi("2", `INFO_SpliceAI` = aboveCutoff),
      RawSpliceAi("3", `INFO_SpliceAI` = aboveCutoff)
    ).toDF())

    val resultDF = job.transformSingle(inputData)

    val expectedResults = Seq(enrichedAboveCutoff("2"), enrichedAboveCutoff("3"))
    resultDF.as[EnrichedSpliceAi].collect() should contain allElementsOf expectedResults
  }

  it should "drop a variant-gene row whose delta score is below the cutoff" in {
    val below = RawSpliceAi(`INFO_SpliceAI` = Seq(spliceaiEntry("QUIET", 0.19, 0.00, 0.00, 0.00)))

    job.transformSingle(Map(source.id -> Seq(below).toDF())).count() shouldBe 0
  }

  it should "keep a variant-gene row whose delta score sits exactly on the cutoff" in {
    // The cutoff is inclusive: 0.2 is Illumina's high-recall threshold, so it is the first score kept.
    val onCutoff = RawSpliceAi(`INFO_SpliceAI` = Seq(spliceaiEntry("EDGE", 0.00, 0.00, 0.20, 0.00)))

    val result = job.transformSingle(Map(source.id -> Seq(onCutoff).toDF()))

    result.count() shouldBe 1
    result.select("max_score.ds").as[Double].head() shouldBe 0.20
  }

  it should "apply the cutoff per gene, not per variant" in {
    // One variant annotated against two genes: the sub-threshold gene goes, the variant stays for the
    // gene that cleared the cutoff.
    val mixed = RawSpliceAi(`INFO_SpliceAI` = Seq(
      spliceaiEntry("QUIET", 0.05, 0.00, 0.00, 0.00),
      spliceaiEntry("LOUD", 0.00, 0.00, 0.00, 0.60)
    ))

    val result = job.transformSingle(Map(source.id -> Seq(mixed).toDF()))

    result.select("symbol").as[String].collect() shouldBe Array("LOUD")
  }

  it should "emit one row per gene for a multi-gene SpliceAI annotation" in {
    // A variant overlapping two genes carries two pipe-delimited entries in INFO_SpliceAI; both must survive.
    // GENE1's strongest score is exactly the 0.2 cutoff, so this also pins the predicate as inclusive.
    val multiGene = RawSpliceAi(`INFO_SpliceAI` = Seq(
      spliceaiEntry("GENE1", 0.10, 0.20, 0.00, 0.00),
      spliceaiEntry("GENE2", 0.50, 0.00, 0.00, 0.00),
    ))

    val result = job.transformSingle(Map(source.id -> Seq(multiGene).toDF()))

    result.count() shouldBe 2
    val perGene = result.select("symbol", "ds_ag", "ds_al").as[(String, Double, Double)].collect().toSet
    perGene shouldBe Set(("GENE1", 0.10, 0.20), ("GENE2", 0.50, 0.00))
  }

  it should "publish the locus_hash join key and drop the intermediate locus (SJRA-1811)" in {
    // The hash covers the locus only, so raising DS_AG past the cutoff keeps the expected digest intact.
    val result = job.transformSingle(Map(source.id -> Seq(RawSpliceAi(`INFO_SpliceAI` = aboveCutoff)).toDF()))

    result.columns should not contain "locus"

    // Glow's 0-based start 210862941 is published as the 1-based POS 210862942, so the locus is
    // "1-210862942-GGCA-G". sha256 of that string:
    result.select("locus_hash").as[String].head() shouldBe
      "b1d4a60d53627d5c87f4b0412cba4006ac88aaf9565861169ec187f4dc3d4754"
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
