package org.radiant.opendatalake.enriched

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, functions}

object SpliceAi {

  def addMaxScore(df: DataFrame): DataFrame = {
    val originalColumns = df.columns.map(col)

    val getDs: Column => Column = _.getItem(0).getField("ds") // Get delta score
    val scoreColumnNames = Array("AG", "AL", "DG", "DL")
    val scoreColumns = scoreColumnNames.map(c => array(struct(col(c) as "ds", lit(c) as "type")))
    val maxScore: Column = scoreColumns.reduce {
      (c1, c2) =>
        when(getDs(c1) > getDs(c2), c1)
          .when(getDs(c1) === getDs(c2), concat(c1, c2))
          .otherwise(c2)
    }

    df
      .select(
        originalColumns :+
          col("ds_ag").as("AG") :+ // acceptor gain
          col("ds_al").as("AL") :+ // acceptor loss
          col("ds_dg").as("DG") :+ // donor gain
          col("ds_dl").as("DL"): _* // donor loss
      )
      .withColumn("max_score_temp", maxScore)
      .withColumn("max_score", struct(
        getDs(col("max_score_temp")) as "ds",
        functions.transform(col("max_score_temp"), c => c.getField("type")) as "type")
      )
      .withColumn("max_score", col("max_score").withField("type", when(col("max_score.ds") === 0, null).otherwise(col("max_score.type"))))
      .select(originalColumns :+ col("max_score"): _*)
  }
}
