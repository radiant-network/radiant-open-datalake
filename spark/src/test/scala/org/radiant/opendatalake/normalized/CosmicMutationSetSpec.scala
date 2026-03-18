package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.models.normalized.NormalizedCosmicMutationSet
import bio.ferlab.datalake.testutils.models.raw.RawCosmicMutationSet
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.normalized.cosmic.CosmicMutationSet
import org.radiant.opendatalake.testutils.SparkSpec

class CosmicMutationSetSpec extends SparkSpec {

  import spark.implicits._

  val source: DatasetConf = conf.getDataset("raw_cosmic_mutation_set")
  val destination: DatasetConf = conf.getDataset("normalized_cosmic_mutation_set")

  it should "normalize cosmic mutation set" in {
    val df = Seq(RawCosmicMutationSet()).toDF()

    val result = CosmicMutationSet(TestETLContext()).transformSingle(Map(source.id -> df))

    result.as[NormalizedCosmicMutationSet].collect() should contain theSameElementsAs Seq(NormalizedCosmicMutationSet())
  }
}



