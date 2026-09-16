package org.radiant.opendatalake.normalized

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, concat_ws, sha2}

object Locus {

  def withLocus(df: DataFrame): DataFrame =
    df.withColumn("locus", concat_ws("-", col("chromosome"), col("start"), col("reference"), col("alternate")))
      .withColumn("locus_hash", sha2(col("locus"), 256))

  /** For contracts whose consumer needs only the hash. */
  def withLocusHash(df: DataFrame): DataFrame = withLocus(df).drop("locus")
}
