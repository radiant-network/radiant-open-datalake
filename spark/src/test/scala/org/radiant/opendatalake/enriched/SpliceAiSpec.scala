package org.radiant.opendatalake.enriched

import bio.ferlab.datalake.testutils.models.enriched.{EnrichedSpliceAi, MAX_SCORE}
import bio.ferlab.datalake.testutils.models.normalized.NormalizedSpliceAi
import org.radiant.opendatalake.testutils.SparkSpec

class SpliceAiSpec extends SparkSpec {

  import spark.implicits._

  "addMaxScore" should "append max_score to NormalizedSpliceAi (EnrichedSpliceAi)" in {
    val inputData = Seq(NormalizedSpliceAi("1"), NormalizedSpliceAi("2")).toDF()

    val resultDF = SpliceAi.addMaxScore(inputData)

    val expected = Seq(EnrichedSpliceAi("1"), EnrichedSpliceAi("2"))
    resultDF.as[EnrichedSpliceAi].collect() shouldBe expected
  }

  "addMaxScore" should "compute max score for each variant-gene" in {
    val inputData = Seq(
      NormalizedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene1", `ds_ag` = 1.0, `ds_al` = 2.00, `ds_dg` = 0.0, `ds_dl` = 0.0),
      NormalizedSpliceAi(`chromosome` = "1", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene2", `ds_ag` = 0.0, `ds_al` = 0.00, `ds_dg` = 0.0, `ds_dl` = 0.0),
      NormalizedSpliceAi(`chromosome` = "2", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene1", `ds_ag` = 1.0, `ds_al` = 1.00, `ds_dg` = 0.0, `ds_dl` = 0.0),
      NormalizedSpliceAi(`chromosome` = "3", `start` = 1, `end` = 2, `reference` = "T", `alternate` = "C", `symbol` = "gene1", `ds_ag` = 1.0, `ds_al` = 1.00, `ds_dg` = 1.0, `ds_dl` = 1.0),
    ).toDF()

    val resultDF = SpliceAi.addMaxScore(inputData)

    val expected = Seq(
      MAX_SCORE(`ds` = 2.00, `type` = Some(Seq("AL"))),
      MAX_SCORE(`ds` = 0.00, `type` = None),
      MAX_SCORE(`ds` = 1.00, `type` = Some(Seq("AG", "AL"))),
      MAX_SCORE(`ds` = 1.00, `type` = Some(Seq("AG", "AL", "DG", "DL"))),
    )

    resultDF
      .select("max_score.*")
      .as[MAX_SCORE].collect() shouldBe expected
  }
}
