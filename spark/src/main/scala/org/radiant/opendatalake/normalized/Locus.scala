package org.radiant.opendatalake.normalized

import org.apache.spark.sql.functions.{col, concat_ws, lit, sha2, upper, when}
import org.apache.spark.sql.{Column, DataFrame}

object Locus {

  private val Mitochondrion = "M"

  private def canonicalChromosome(chromosome: Column): Column =
    when(upper(chromosome) === "MT", lit(Mitochondrion)).otherwise(chromosome)

  def withLocus(df: DataFrame): DataFrame =
    df.withColumn("chromosome", canonicalChromosome(col("chromosome")))
      .withColumn("locus", concat_ws("-", col("chromosome"), col("start"), col("reference"), col("alternate")))
      .withColumn("locus_hash", sha2(col("locus"), 256))

  /** For contracts whose consumer needs only the hash. */
  def withLocusHash(df: DataFrame): DataFrame = withLocus(df).drop("locus")
}
