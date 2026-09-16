package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.SparkSpec

// dbSNP ships RefSeq accessions as contig names; Glow's `start` is 0-based, so 69090 here is VCF POS 69091.
case class RawDbsnpInput(contigName: String = "NC_000001.11",
                         start: Long = 69090,
                         end: Long = 69091,
                         names: Seq[String] = Seq("rs1234"),
                         referenceAllele: String = "A",
                         alternateAlleles: Seq[String] = Seq("G"))

class DBSNP_v1Spec extends SparkSpec {

  import spark.implicits._

  private val source: DatasetConf = conf.getDataset("raw_dbsnp")

  private def job = DBSNP_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "dbsnp")

  assert(
    job.mainDestination.table.map(_.name).contains("dbsnp_v1"),
    s"MAJOR 1 must publish to dbsnp_v1, not ${job.mainDestination.table}"
  )

  "transformSingle" should "map the RefSeq accession to a bare chromosome and publish the locus_hash join key" in {
    val result = job.transformSingle(Map(source.id -> Seq(RawDbsnpInput()).toDF()))

    result.columns should not contain "locus"

    val row = result.select("chromosome", "start", "alternate", "locus_hash").head()
    row.getString(0) shouldBe "1"
    // The invariant: `start` is the 1-based VCF POS, not Glow's 0-based `start`.
    row.getLong(1) shouldBe 69091L
    row.getString(2) shouldBe "G"
    // sha256("1-69091-A-G"), the same vector dbnsfp_v1 publishes.
    row.getString(3) shouldBe "f3d6cd97737ce4ce596e06c07782948838ab00f2545372e5ecc734207a26eab6"
  }

  it should "decode the sex and mitochondrial accessions" in {
    val raw = Seq(
      RawDbsnpInput(contigName = "NC_000023.11"),
      RawDbsnpInput(contigName = "NC_000024.10"),
      RawDbsnpInput(contigName = "NC_012920.1")
    ).toDF()

    job.transformSingle(Map(source.id -> raw)).select("chromosome").as[String].collect().toSet shouldBe
      Set("X", "Y", "M")
  }

  it should "drop contigs that are not RefSeq accessions" in {
    val raw = Seq(RawDbsnpInput(), RawDbsnpInput(contigName = "NT_187361.1")).toDF()

    job.transformSingle(Map(source.id -> raw)).count() shouldBe 1
  }

  it should "emit one row per alternate allele" in {
    val raw = Seq(RawDbsnpInput(alternateAlleles = Seq("G", "T"))).toDF()

    val hashes = job.transformSingle(Map(source.id -> raw)).select("locus_hash").as[String].collect()

    // Distinct alternates must hash apart -- the hash is the full locus, not just the position.
    hashes should have length 2
    hashes.distinct should have length 2
    hashes should contain("f3d6cd97737ce4ce596e06c07782948838ab00f2545372e5ecc734207a26eab6")
  }
}
