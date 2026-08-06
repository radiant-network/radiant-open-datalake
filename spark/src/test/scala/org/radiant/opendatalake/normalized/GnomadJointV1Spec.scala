package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.apache.spark.sql.Row
import org.radiant.opendatalake.normalized.gnomad.GnomadJoint_v1
import org.radiant.opendatalake.testutils.SparkSpec

/*
  Reads the real gnomAD joint header and two hand-picked chr22 records from
  `input_vcf/gnomadV4Joint.vcf`: one variant seen only in the genomes, one seen in both callsets.
  Going through Glow rather than a case class is deliberate — the projection depends on ~300 INFO
  fields being flattened, which a hand-written model would not reproduce.
 */
class GnomadJointV1Spec extends SparkSpec {

  private val source: DatasetConf = conf.getDataset("raw_gnomad_joint")

  private def job =
    GnomadJoint_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "gnomad_joint")

  assert(
    job.mainDestination.table.map(_.name).contains("gnomad_joint_v1"),
    s"MAJOR 1 must publish to gnomad_joint_v1, not ${job.mainDestination.table}"
  )

  "transformSingle" should "publish the joint, genomes and exomes metrics of each record" in {
    val fixture = getClass.getResource("/input_vcf/gnomadV4Joint.vcf").getPath
    val raw = spark.read.format("vcf").option("flattenInfoFields", "true").load(fixture)

    val rows: Array[Row] = job.transformSingle(Map(source.id -> raw)).orderBy("start").collect()
    rows should have length 2

    rows.head.schema.fieldNames should contain noneOf ("qual", "name")

    // First row: only genomes and joint metrics are present
    // Should match: chr22:10510033 T>C
    val genomesOnly = rows(0)
    genomesOnly.getAs[String]("chromosome") shouldBe "22"
    genomesOnly.getAs[Long]("start") shouldBe 10510033L
    genomesOnly.getAs[String]("reference") shouldBe "T"
    genomesOnly.getAs[String]("alternate") shouldBe "C"
    genomesOnly.getAs[Long]("ac_joint") shouldBe 0L
    genomesOnly.getAs[Long]("an_joint") shouldBe 6L
    genomesOnly.getAs[Long]("hom_joint") shouldBe 0L
    genomesOnly.getAs[Long]("ac_genomes") shouldBe 0L
    genomesOnly.getAs[Long]("an_genomes") shouldBe 6L
    genomesOnly.getAs[Long]("hom_genomes") shouldBe 0L
    for (column <- Seq("ac_exomes", "af_exomes", "an_exomes", "hom_exomes")) {
      withClue(s"$column should be null when the variant is absent from the exomes: ") {
        val fieldIndex = genomesOnly.fieldIndex(column)
        genomesOnly.isNullAt(fieldIndex) shouldBe true
      }
    }

    // Second row: genomes, exomes and joint metrics are present
    // Should match: chr22:10736031 G>A
    val bothCallsets = rows(1)
    bothCallsets.getAs[String]("chromosome") shouldBe "22"
    bothCallsets.getAs[Long]("start") shouldBe 10736031L
    bothCallsets.getAs[String]("reference") shouldBe "G"
    bothCallsets.getAs[String]("alternate") shouldBe "A"
    bothCallsets.getAs[Long]("an_joint") shouldBe 152298L
    bothCallsets.getAs[Long]("an_genomes") shouldBe 152296L
    bothCallsets.getAs[Long]("an_exomes") shouldBe 2L
    bothCallsets.getAs[Long]("hom_joint") shouldBe 0L
    bothCallsets.getAs[Long]("hom_genomes") shouldBe 0L
    bothCallsets.getAs[Long]("hom_exomes") shouldBe 0L
  }
}
