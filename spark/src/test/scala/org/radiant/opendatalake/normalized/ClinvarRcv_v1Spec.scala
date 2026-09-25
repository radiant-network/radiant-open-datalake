package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

import java.sql.Date

case class ClinvarRcvSubmission(submitter: Option[String],
                                scv: Option[String],
                                version: Option[Int],
                                review_status: Option[String],
                                review_status_stars: Int,
                                clinical_significance: Option[String],
                                date_last_evaluated: Option[Date])

case class NormalizedClinvarRcv(clinvar_id: Option[String] = Some("18397"),
                                accession: Option[String] = Some("RCV000000010"),
                                version: Option[Int] = Some(3),
                                clinical_significance: Option[Seq[String]] = Some(Seq("Pathogenic")),
                                date_last_evaluated: Option[Date] = Some(Date.valueOf("2000-04-01")),
                                submission_count: Option[Int] = Some(1),
                                review_status: Option[String] = Some("no assertion criteria provided"),
                                review_status_stars: Int = 0,
                                traits: Seq[String] = Seq("DUFFY BLOOD GROUP SYSTEM, FY(a-b-) PHENOTYPE"),
                                origins: Seq[String] = Seq("germline"),
                                submissions: Seq[ClinvarRcvSubmission] = Seq(),
                                clinical_significance_count: Map[String, Int] = Map("Pathogenic" -> 1))

class ClinvarRcv_v1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  private val clinvarRcvXml: DatasetConf = conf.getDataset("raw_clinvar_rcv")

  // The version the sample release is filed under in test resources: `clinvar_rcv/test/`.
  private val version = "test"

  // `extract` swaps the raw storage root for the runtime one, so the spec hands it the same test
  // resources root the test configuration is built with.
  private val rawStorage: String = getClass.getClassLoader.getResource(".").getFile

  private def job = ClinvarRcv_v1(TestETLContext(), version, rawStorage, tablePrefix = "clinvar_rcv")

  private val destination: DatasetConf = job.mainDestination
  assert(
    destination.table.map(_.name).contains("clinvar_rcv_v1"),
    s"MAJOR 1 must publish to clinvar_rcv_v1, not ${destination.table}"
  )

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  private def normalized: Map[String, org.apache.spark.sql.DataFrame] = job.extract()

  private def transformed: Seq[NormalizedClinvarRcv] =
    job.transformSingle(normalized).as[NormalizedClinvarRcv].collect().toSeq

  private def byAccession(accession: String): NormalizedClinvarRcv =
    transformed.find(_.accession.contains(accession)).getOrElse(fail(s"no row for $accession"))

  "extract" should "read one row per ClinVarSet of the release" in {
    normalized(clinvarRcvXml.id).count() shouldBe 4
  }

  "transform" should "publish the aggregate record with its single submission" in {
    byAccession("RCV000000010") shouldBe NormalizedClinvarRcv(
      submissions = Seq(
        ClinvarRcvSubmission(
          submitter = Some("OMIM"),
          scv = Some("SCV000020153"),
          version = Some(2),
          review_status = Some("no assertion criteria provided"),
          review_status_stars = 0,
          clinical_significance = Some("Pathogenic"),
          date_last_evaluated = Some(Date.valueOf("2000-04-01"))
        )
      )
    )
  }

  it should "split a '/'-joined aggregate classification and count only the submissions behind it" in {
    val row = byAccession("RCV000000469")

    row.clinical_significance shouldBe Some(Seq("Pathogenic", "Likely pathogenic"))
    // The fourth submission classifies Uncertain significance, which ClinVar did not carry into the
    // aggregate: it is absent from the map rather than counted at zero.
    row.clinical_significance_count shouldBe Map("Pathogenic" -> 2, "Likely pathogenic" -> 1)
    row.submission_count shouldBe Some(4)
    row.submissions should have size 4
  }

  it should "rate the review status in stars, on the record and on each submission" in {
    byAccession("RCV000000469").review_status_stars shouldBe 2 // multiple submitters, no conflicts
    byAccession("RCV000000471").review_status_stars shouldBe 4 // practice guideline
    byAccession("RCV000000010").review_status_stars shouldBe 0 // no assertion criteria provided

    byAccession("RCV000000469").submissions.map(s => s.review_status -> s.review_status_stars) should
      contain theSameElementsAs Seq(
        Some("no assertion criteria provided") -> 0,
        Some("criteria provided, single submitter") -> 1,
        Some("reviewed by expert panel") -> 3,
        Some("criteria provided, single submitter") -> 1
      )
  }

  it should "keep only preferred trait names, in document order" in {
    byAccession("RCV000000469").traits shouldBe Seq("Hereditary hemochromatosis", "Hemochromatosis type 1")
  }

  it should "deduplicate and sort the observed origins" in {
    byAccession("RCV000000469").origins shouldBe Seq("de novo", "germline")
  }

  it should "take the variation id from the first variant of a GenotypeSet" in {
    // A compound heterozygote carries a GenotypeSet rather than a MeasureSet.
    byAccession("RCV000000471").clinvar_id shouldBe Some("9")
  }

  it should "leave the classification null when the record carries none" in {
    val row = byAccession("RCV000000472")

    row.clinical_significance shouldBe None
    row.date_last_evaluated shouldBe None
    row.submission_count shouldBe None
    row.review_status shouldBe None
    row.review_status_stars shouldBe 0
    withClue("no ObservedIn must read as no origins, not as null: ") {
      row.origins shouldBe empty
    }
    row.clinical_significance_count shouldBe empty
  }

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) =
    spark.read.option("branch", branch).table(tableName).as[NormalizedClinvarRcv]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(NormalizedClinvarRcv(accession = Some("RCV1")))
    val secondLoad = firstLoad :+ NormalizedClinvarRcv(accession = Some("RCV2"))

    job.loadSingle(firstLoad.toDF())
    onBranch(version).collect() should contain theSameElementsAs firstLoad

    // Re-importing the same dataset_version replaces the branch rather than merging into it (§3.4).
    job.loadSingle(secondLoad.toDF())
    onBranch(version).collect() should contain theSameElementsAs secondLoad

    withClue("main must stay empty — consumers read the dataset_version branch: ") {
      spark.table(tableName).count() shouldBe 0
    }

    val refs = spark.sql(s"SELECT name FROM $tableName.refs").collect().map(_.getString(0)).toSet
    refs should contain allOf ("main", version)
    withClue(s"the transient audit branch outlived the import, refs were $refs: ") {
      refs.filter(_.startsWith("audit")) shouldBe empty
    }
  }
}
