package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import bio.ferlab.datalake.testutils.models.normalized.NormalizedGnomadConstraint
import bio.ferlab.datalake.testutils.models.raw.RawGnomadConstraint
import org.radiant.opendatalake.normalized.gnomad.GnomadConstraint_v1
import org.radiant.opendatalake.testutils.SparkSpec

class GnomadConstraintV1Spec extends SparkSpec {

  import spark.implicits._

  private val source: DatasetConf = conf.getDataset("raw_gnomad_constraint")

  private def job =
    GnomadConstraint_v1(TestETLContext(), version = "2.1.1", rawStorage = "", tablePrefix = "gnomad_constraint")

  assert(
    job.mainDestination.table.map(_.name).contains("gnomad_constraint_v1"),
    s"MAJOR 1 must publish to gnomad_constraint_v1, not ${job.mainDestination.table}"
  )

  "transform" should "transform RawGnomadConstraint to NormalizedGnomadConstraint" in {
    val inputData = Map(source.id -> Seq(RawGnomadConstraint()).toDF())

    val resultDF = job.transformSingle(inputData)

    val expectedResults = Seq(NormalizedGnomadConstraint())
    resultDF.as[NormalizedGnomadConstraint].collect() shouldBe expectedResults
  }

}
