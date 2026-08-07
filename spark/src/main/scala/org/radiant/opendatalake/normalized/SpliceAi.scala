package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.spark3.implicits.GenomicImplicits.columns._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Pure normalize transform for the SpliceAI precomputed scores, shared by the contract normalizer
 * [[SpliceAi_v1]]. Splits the pipe-delimited `SpliceAI` INFO field into typed columns; one row per
 * variant-gene pair. Illumina's SNV and indel VCFs share this schema, so a single call normalizes both.
 */
object SpliceAi {

  def normalize(df: DataFrame): DataFrame =
    df
      .select(
        chromosome +:
          start +:
          end +:
          reference +:
          alternate +:
          flattenInfo(df, except = "INFO_OLD_MULTIALLELIC", "INFO_FILTERS"): _*
      )
      .withColumn("spliceai", split(col("spliceai"), "\\|"))
      .withColumn("allele", col("spliceai")(0))
      .withColumn("symbol", col("spliceai")(1))
      .withColumn("ds_ag", col("spliceai")(2).cast("double"))
      .withColumn("ds_al", col("spliceai")(3).cast("double"))
      .withColumn("ds_dg", col("spliceai")(4).cast("double"))
      .withColumn("ds_dl", col("spliceai")(5).cast("double"))
      .withColumn("dp_ag", col("spliceai")(6).cast("int"))
      .withColumn("dp_al", col("spliceai")(7).cast("int"))
      .withColumn("dp_dg", col("spliceai")(8).cast("int"))
      .withColumn("dp_dl", col("spliceai")(9).cast("int"))
      .drop("spliceai")
}
