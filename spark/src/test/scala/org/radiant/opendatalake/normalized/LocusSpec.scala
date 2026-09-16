package org.radiant.opendatalake.normalized

import org.radiant.opendatalake.testutils.SparkSpec

/*
  The single definition of the join key every variant contract publishes (SJRA-1811). The per-source
  specs assert the hash end-to-end over their own raw fixture -- that is what catches a source whose
  `start` is not the 1-based VCF POS. This spec pins the expression itself.
 */
class LocusSpec extends SparkSpec {

  import spark.implicits._

  private def df = Seq(("1", 69091L, "A", "G")).toDF("chromosome", "start", "reference", "alternate")

  "withLocus" should "append the platform locus / locus_hash join key" in {
    val row = Locus.withLocus(df).select("locus", "locus_hash").head()

    // Must match the consumer side: "chrom-pos-ref-alt" from the 1-based VCF POS, sha256 lowercase hex.
    // Independently verified against Python hashlib.sha256(b"1-69091-A-G").
    row.getString(0) shouldBe "1-69091-A-G"
    row.getString(1) shouldBe "f3d6cd97737ce4ce596e06c07782948838ab00f2545372e5ecc734207a26eab6"
  }

  "withLocusHash" should "keep the hash and drop the intermediate locus string" in {
    val result = Locus.withLocusHash(df)

    result.columns should contain("locus_hash")
    result.columns should not contain "locus"
    result.select("locus_hash").head().getString(0) shouldBe
      "f3d6cd97737ce4ce596e06c07782948838ab00f2545372e5ecc734207a26eab6"
  }
}
