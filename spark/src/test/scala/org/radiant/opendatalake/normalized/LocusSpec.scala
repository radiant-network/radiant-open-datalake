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

  private def mito(chromosome: String) =
    Seq((chromosome, 3243L, "A", "G")).toDF("chromosome", "start", "reference", "alternate")

  "withLocus" should "append the platform locus / locus_hash join key" in {
    val row = Locus.withLocus(df).select("locus", "locus_hash").head()

    // Must match the consumer side: "chrom-pos-ref-alt" from the 1-based VCF POS, sha256 lowercase hex.
    // Independently verified against Python hashlib.sha256(b"1-69091-A-G").
    row.getString(0) shouldBe "1-69091-A-G"
    row.getString(1) shouldBe "f3d6cd97737ce4ce596e06c07782948838ab00f2545372e5ecc734207a26eab6"
  }

  it should "canonicalise the mitochondrion so MT and M are the same variant" in {
    // ClinVar ships bare contigs and so says "MT"; gnomAD/TopMed/dbSNP say "M". Both must reach the same
    // key or `variant_lookup` silently misses every ClinVar mitochondrial variant.
    val asMT = Locus.withLocus(mito("MT")).select("chromosome", "locus", "locus_hash").head()
    val asM = Locus.withLocus(mito("M")).select("chromosome", "locus", "locus_hash").head()

    // The published column is rewritten too -- chromosome, locus and locus_hash must not disagree.
    asMT.getString(0) shouldBe "M"
    asMT.getString(1) shouldBe "M-3243-A-G"
    // sha256("M-3243-A-G"), NOT sha256("MT-3243-A-G").
    asMT.getString(2) shouldBe "e91f958c747b3575aa5d191ae61a4c4e501fa516f4068212177caadc4a0bc26b"
    asMT shouldBe asM
  }

  it should "leave every other chromosome untouched" in {
    val chromosomes = Seq("1", "22", "X", "Y").map(c => Locus.withLocus(mito(c)).select("chromosome").as[String].head())

    chromosomes shouldBe Seq("1", "22", "X", "Y")
  }

  "withLocusHash" should "keep the hash and drop the intermediate locus string" in {
    val result = Locus.withLocusHash(df)

    result.columns should contain("locus_hash")
    result.columns should not contain "locus"
    result.select("locus_hash").head().getString(0) shouldBe
      "f3d6cd97737ce4ce596e06c07782948838ab00f2545372e5ecc734207a26eab6"
  }
}
