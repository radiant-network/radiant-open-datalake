package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

// Minimal locus-shaped row; the full per-transcript schema (~130 cols) is exercised end-to-end by
// EMR, not unit tests -- here we cover the WAP publish mechanics and the locus/locus_hash join key.
case class NormalizedDbnsfp(chromosome: String = "1",
                            start: Long = 69091,
                            reference: String = "A",
                            alternate: String = "G",
                            aaref: String = "M")

class DBNSFP_v1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  private def job = DBNSFP_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "dbnsfp")

  private val destination: DatasetConf = job.mainDestination
  assert(
    destination.table.map(_.name).contains("dbnsfp_v1"),
    s"MAJOR 1 must publish to dbnsfp_v1, not ${destination.table}"
  )

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "withLocus" should "append the platform locus / locus_hash join key" in {
    val df = Seq(("1", 69091L, "A", "G")).toDF("chromosome", "start", "reference", "alternate")

    val row = job.withLocus(df).select("locus", "locus_hash").head()

    // Must match the occurrence side (process_common: "chrom-pos-ref-alt", sha256 hex).
    row.getString(0) shouldBe "1-69091-A-G"
    row.getString(1) shouldBe "f3d6cd97737ce4ce596e06c07782948838ab00f2545372e5ecc734207a26eab6"
  }

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName).as[NormalizedDbnsfp]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(NormalizedDbnsfp(alternate = "G"))
    val secondLoad = Seq(
      NormalizedDbnsfp(alternate = "G"),
      NormalizedDbnsfp(start = 100, alternate = "T")
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
