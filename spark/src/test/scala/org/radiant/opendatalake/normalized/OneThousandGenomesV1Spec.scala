package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import bio.ferlab.datalake.testutils.models.normalized.NormalizedOneKGenomes
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

case class RawOneKGenomesInput(contigName: String = "chr1",
                               start: Long = 69896,
                               end: Long = 69897,
                               names: Seq[String] = Seq("rs200676709"),
                               referenceAllele: String = "T",
                               alternateAlleles: Seq[String] = Seq("C"),
                               INFO_AC: Seq[Int] = Seq(3446),
                               INFO_AF: Seq[Double] = Seq(0.688099),
                               INFO_AN: Int = 5008,
                               INFO_AFR_AF: Seq[Double] = Seq(0.407),
                               INFO_EUR_AF: Seq[Double] = Seq(0.7942),
                               INFO_SAS_AF: Seq[Double] = Seq(0.8098),
                               INFO_AMR_AF: Seq[Double] = Seq(0.6254),
                               INFO_EAS_AF: Seq[Double] = Seq(0.876),
                               INFO_DP: Int = 22289)

class OneThousandGenomesV1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  private val source: DatasetConf = conf.getDataset("raw_1000_genomes")

  private def job =
    new OneThousandGenomes_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "1000_genomes")

  private val destination: DatasetConf = job.mainDestination
  assert(
    destination.table.map(_.name).contains("1000_genomes_v1"),
    s"MAJOR 1 must publish to 1000_genomes_v1, not ${destination.table}"
  )

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "transform" should "narrow the flattened 1000 Genomes VCF to the normalized locus + population AF columns" in {
    val inputData = Map(source.id -> Seq(RawOneKGenomesInput()).toDF())

    val result = job.transformSingle(inputData).as[NormalizedOneKGenomes].collect()

    result should contain theSameElementsAs Seq(NormalizedOneKGenomes())
  }

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName).as[NormalizedOneKGenomes]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(NormalizedOneKGenomes(name = "rs1"))
    val secondLoad = Seq(
      NormalizedOneKGenomes(name = "rs1", af = 0.5),
      NormalizedOneKGenomes(name = "rs2", start = 100, end = 101)
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
