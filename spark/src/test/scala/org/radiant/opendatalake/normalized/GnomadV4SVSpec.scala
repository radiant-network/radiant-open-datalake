package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import org.radiant.opendatalake.normalized.gnomad.GnomadV4SV
import bio.ferlab.datalake.testutils.models.normalized.NormalizedGnomadV4SV
import bio.ferlab.datalake.testutils.models.raw.RawGnomadV4SV
import bio.ferlab.datalake.testutils.{SparkSpec, TestETLContext}
import org.radiant.opendatalake.testutils.WithTestConfig

class GnomadV4SVSpec extends SparkSpec with WithTestConfig {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_gnomad_sv_v4")
  val destination: DatasetConf = conf.getDataset("normalized_gnomad_sv_v4")

  "transform" should "transform RawGnomadV4SV to NormalizedGnomadV4SV" in {
    val inputData = Map(source.id -> Seq(RawGnomadV4SV()).toDF())

    val resultDF = new GnomadV4SV(TestETLContext()).transformSingle(inputData)

    val expectedResults = Seq(NormalizedGnomadV4SV())
    resultDF.as[NormalizedGnomadV4SV].collect() shouldBe expectedResults
  }

}
