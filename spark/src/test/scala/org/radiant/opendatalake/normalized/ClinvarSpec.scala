package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.spark3.implicits.DatasetConfImplicits._
import bio.ferlab.datalake.testutils.models.normalized.NormalizedClinvar
import bio.ferlab.datalake.testutils.models.raw.RawClinvar
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}


class ClinvarSpec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_clinvar")
  val destination: DatasetConf = conf.getDataset("normalized_clinvar")

  assert(destination.table.isDefined, "table normalized_clinvar dataset (destination) must be defined in test config")
  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  "transform" should "transform ClinvarInput to ClinvarOutput" in {
    val inputData = Map(source.id -> Seq(RawClinvar("2"), RawClinvar("3")).toDF())

    val resultDF = new Clinvar(TestETLContext()).transformSingle(inputData)

    val expectedResults = Seq(NormalizedClinvar("2"), NormalizedClinvar("3"))

    resultDF.as[NormalizedClinvar].collect() should contain allElementsOf expectedResults
  }

  "load" should "overwrite data" in {
    val firstLoad = Seq(NormalizedClinvar("1", name = "first"), NormalizedClinvar("2"))
    val secondLoad = Seq(NormalizedClinvar("1", name = "second"), NormalizedClinvar("3"))
    val expectedResults = Seq(NormalizedClinvar("1", name = "second"), NormalizedClinvar("3"))

    val job = new Clinvar(TestETLContext())
    job.loadSingle(firstLoad.toDF())
    val firstResult = destination.read
    firstResult.select("chromosome", "start", "end", "reference", "alternate", "name").show(false)
    firstResult.as[NormalizedClinvar].collect() should contain allElementsOf firstLoad

    job.loadSingle(secondLoad.toDF())
    val secondResult = destination.read
    secondResult.select("chromosome", "start", "end", "reference", "alternate", "name").show(false)
    secondResult.as[NormalizedClinvar].collect() should contain allElementsOf expectedResults

    val snapshots = spark.sql("SELECT * FROM reference.clinvar.snapshots").collect()
    snapshots.length shouldBe 2 // One for each overwrite
  }
}
