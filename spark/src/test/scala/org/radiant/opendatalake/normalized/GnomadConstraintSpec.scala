package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import org.radiant.opendatalake.normalized.gnomad.GnomadConstraint
import bio.ferlab.datalake.testutils.models.normalized.NormalizedGnomadConstraint
import bio.ferlab.datalake.testutils.models.raw.RawGnomadConstraint
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.radiant.opendatalake.testutils.WithTestConfig

class GnomadConstraintSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_gnomad_constraint_v2_1_1")
  val destination: DatasetConf = conf.getDataset("normalized_gnomad_constraint_v2_1_1")

  "transform" should "transform RawGnomadConstraint to NormalizedGnomadConstraint" in {
    val inputData = Map(source.id -> Seq(RawGnomadConstraint()).toDF())

    val resultDF = new GnomadConstraint(TestETLContext()).transformSingle(inputData)

//    ClassGenerator
//      .writeCLassFile(
//        "bio.ferlab.datalake.testutils.models.normalized",
//        "NormalizedGnomadConstraint",
//        resultDF,
//        "datalake-spark3/src/test/scala/")

    val expectedResults = Seq(NormalizedGnomadConstraint())
    resultDF.as[NormalizedGnomadConstraint].collect() shouldBe expectedResults
  }

}
