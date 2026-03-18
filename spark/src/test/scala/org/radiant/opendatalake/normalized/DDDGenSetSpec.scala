package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.models.normalized.{NormalizedCosmicGeneSet, NormalizedDddGeneCensus}
import bio.ferlab.datalake.testutils.models.raw.RawDDDGeneSet
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.SparkSpec

class DDDGenSetSpec extends SparkSpec {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_ddd_gene_set")
  val destination: DatasetConf = conf.getDataset("normalized_ddd_gene_set")

  "transform" should "transform DDD Gene input to DDD Gene output" in {
    val df = Seq(RawDDDGeneSet()).toDF()

    val result = DDDGeneSet(TestETLContext()).transformSingle(Map(source.id -> df))

    result.as[NormalizedDddGeneCensus].collect() should contain theSameElementsAs Seq(NormalizedDddGeneCensus())
  }
}



