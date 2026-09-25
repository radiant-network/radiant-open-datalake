package org.radiant.opendatalake.enriched

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object SpliceAi {

  // Delta-score column -> event type, in the order ties are reported.
  private val scores: Seq[(String, String)] = Seq(
    "ds_ag" -> "AG", // acceptor gain
    "ds_al" -> "AL", // acceptor loss
    "ds_dg" -> "DG", // donor gain
    "ds_dl" -> "DL"  // donor loss
  )

  /** Appends `max_score {ds, type}`: the strongest of the four delta scores and every event tied at it.
    *
    * Expression size is linear in the number of scores. Keep it that way — a filter on `max_score.ds`
    * is pushed down with the expression inlined, so its size is paid per row. The previous
    * `reduce(when/concat)` form grew 4x per score and made that filter cost ~3x the job's runtime. */
  def addMaxScore(df: DataFrame): DataFrame = {
    // Illumina: delta score = max(DS_AG, DS_AL, DS_DG, DS_DL). `greatest` skips nulls.
    val ds: Column = greatest(scores.map { case (c, _) => col(c) }: _*)
    // Non-null struct elements, so `type` stays array<string not null> after filter/transform.
    val events: Column = array(scores.map { case (c, t) => struct(col(c) as "ds", lit(t) as "type") }: _*)
    val tied: Column = transform(filter(events, e => e.getField("ds") === ds), e => e.getField("type"))

    df.withColumn("max_score", struct(
      ds as "ds",
      when(ds === 0, lit(null)).otherwise(tied) as "type"
    ))
  }
}
