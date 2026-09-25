package org.radiant.opendatalake.normalized.ensembl

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.apache.spark.sql.{DataFrame, Row}
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

/*
  The fixture is a trimmed excerpt of Ensembl release 114 (Homo_sapiens.GRCh38.114.gff3.gz), kept verbatim:
  the chromosome 1 region line, one biological_region, and gene HES3 (ENSG00000173673) with its two
  transcripts. ENST00000377898 is the canonical MANE Select one; exons ENSE00001182530 and ENSE00001475445
  are shared by both transcripts. It sits at the versioned raw path so `extract` is exercised as on EMR.
 */
class EnsemblV1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  private val version = "114"
  private val rawStorage: String = getClass.getResource("/raw/landing").getPath

  private val HES3 = "ENSG00000173673"
  private val ManeTranscript = "ENST00000377898"
  private val OtherTranscript = "ENST00000706530"

  private def gene = EnsemblGene_v1(TestETLContext(), version, rawStorage, tablePrefix = "ensembl_gene")
  private def transcript = EnsemblTranscript_v1(TestETLContext(), version, rawStorage, tablePrefix = "ensembl_transcript")
  private def exon = EnsemblExon_v1(TestETLContext(), version, rawStorage, tablePrefix = "ensembl_exon")
  private def exonByGene = EnsemblExonByGene_v1(TestETLContext(), version, rawStorage, tablePrefix = "ensembl_exon_by_gene")

  private def jobs: Seq[(ContractETLP, String)] = Seq(
    gene -> "ensembl_gene_v1",
    transcript -> "ensembl_transcript_v1",
    exon -> "ensembl_exon_v1",
    exonByGene -> "ensembl_exon_by_gene_v1"
  )

  private val destinations: List[DatasetConf] = jobs.map(_._1.mainDestination).toList

  jobs.foreach { case (job, expected) =>
    assert(
      job.mainDestination.table.map(_.name).contains(expected),
      s"MAJOR 1 must publish to $expected, not ${job.mainDestination.table}"
    )
  }

  override val dbToCreate: List[String] = destinations.flatMap(_.table.map(_.database)).distinct
  override val dsToClean: List[DatasetConf] = destinations

  private def transformed(job: ContractETLP): DataFrame = job.transformSingle(job.extract())

  "gene" should "keep only gene features, 1-based, with the gene attributes" in {
    val rows = transformed(gene).collect()
    rows should have length 1

    val hes3 = rows.head
    hes3.getAs[String]("gene_id") shouldBe HES3
    hes3.getAs[String]("chromosome") shouldBe "1"
    hes3.getAs[Long]("start") shouldBe 6244179L
    hes3.getAs[Long]("end") shouldBe 6245578L
    hes3.getAs[Long]("length") shouldBe 1400L
    hes3.getAs[Int]("version") shouldBe 9
    hes3.getAs[String]("type") shouldBe "gene"
    hes3.getAs[String]("name") shouldBe "HES3"
    hes3.getAs[String]("biotype") shouldBe "protein_coding"
    hes3.getAs[String]("logic_name") shouldBe "ensembl_havana_gene_homo_sapiens"
  }

  "transcript" should "attach the parent gene and derive canonical / MANE flags from the tags" in {
    val rows = transformed(transcript).collect().map(r => r.getAs[String]("transcript_id") -> r).toMap
    rows.keySet shouldBe Set(ManeTranscript, OtherTranscript)
    rows.values.map(_.getAs[String]("gene_id")).toSet shouldBe Set(HES3)

    val mane = rows(ManeTranscript)
    mane.getAs[Boolean]("is_canonical") shouldBe true
    mane.getAs[Boolean]("is_mane_select") shouldBe true
    mane.getAs[Boolean]("is_mane_plus_clinical") shouldBe false
    mane.getAs[Seq[String]]("tags") should contain theSameElementsInOrderAs
      Seq("gencode_basic", "gencode_primary", "Ensembl_canonical", "MANE_Select")
    mane.getAs[String]("ccdsid") shouldBe "CCDS41238.1"
    mane.getAs[Int]("version") shouldBe 4

    val other = rows(OtherTranscript)
    other.getAs[Boolean]("is_canonical") shouldBe false
    other.getAs[Boolean]("is_mane_select") shouldBe false
    other.getAs[Seq[String]]("tags") shouldBe Seq("gencode_basic")
  }

  "exon" should "emit one row per (exon, transcript), carrying the transcript's gene and flags" in {
    val rows = transformed(exon).collect()
    rows should have length 8
    rows.map(_.getAs[String]("gene_id")).toSet shouldBe Set(HES3)

    val shared = rows.filter(_.getAs[String]("exon_id") == "ENSE00001182530")
    shared.map(_.getAs[String]("transcript_id")).toSet shouldBe Set(ManeTranscript, OtherTranscript)
    shared.find(_.getAs[String]("transcript_id") == ManeTranscript).get.getAs[Boolean]("is_mane_select") shouldBe true
    shared.find(_.getAs[String]("transcript_id") == OtherTranscript).get.getAs[Boolean]("is_mane_select") shouldBe false
    shared.head.getAs[Long]("start") shouldBe 6244548L
    shared.head.getAs[Long]("length") shouldBe 82L // 6244548..6244629, both ends inclusive
    shared.head.getAs[String]("rank") shouldBe "3"
  }

  it should "carry tags and transcript_support_level from the parent transcript" in {
    val exonRows = transformed(exon).collect()
    val transcriptRows = transformed(transcript).collect().map(r => r.getAs[String]("transcript_id") -> r).toMap

    exonRows.foreach { e =>
      val parent = transcriptRows(e.getAs[String]("transcript_id"))
      withClue(s"exon ${e.getAs[String]("exon_id")} of ${e.getAs[String]("transcript_id")}: ") {
        e.getAs[Seq[String]]("tags") shouldBe parent.getAs[Seq[String]]("tags")
        e.getAs[String]("transcript_support_level") shouldBe parent.getAs[String]("transcript_support_level")
      }
    }

    val maneExon = exonRows.find(_.getAs[String]("transcript_id") == ManeTranscript).get
    maneExon.getAs[Seq[String]]("tags") should contain allOf("Ensembl_canonical", "MANE_Select")
    maneExon.getAs[String]("transcript_support_level") shouldBe "2 (assigned to previous version 3)"
  }

  it should "not publish attributes Ensembl never sets on exon rows" in {
    transformed(exon).columns should contain noneOf("alias", "description", "external_name", "logic_name", "phase")
  }

  "exon_by_gene" should "collapse exons shared by several transcripts into one row per (gene, exon)" in {
    val rows: Map[String, Row] = transformed(exonByGene).collect().map(r => r.getAs[String]("exon_id") -> r).toMap
    rows should have size 6

    rows("ENSE00001182530").getAs[Seq[String]]("transcript_ids") should contain theSameElementsAs
      Seq(ManeTranscript, OtherTranscript)
    rows("ENSE00001475445").getAs[Seq[String]]("transcript_ids") should contain theSameElementsAs
      Seq(ManeTranscript, OtherTranscript)
    rows("ENSE00003996080").getAs[Seq[String]]("transcript_ids") shouldBe Seq(ManeTranscript)
    rows.values.map(_.getAs[String]("gene_id")).toSet shouldBe Set(HES3)
  }

  it should "publish only exon-level attributes, identical across the transcripts of an exon" in {
    transformed(exonByGene).columns should contain theSameElementsInOrderAs Seq(
      "gene_id", "exon_id", "chromosome", "start", "end", "transcript_ids", "version", "type", "strand", "name",
      "constitutive", "ensembl_end_phase", "ensembl_phase", "length"
    )
  }

  "load" should "publish every table on the release branch, tag it latest and leave main empty" in {
    jobs.foreach { case (job, _) =>
      job.loadSingle(transformed(job))

      val tableName = job.mainDestination.table.map(_.fullName).get
      withClue(s"$tableName: ") {
        spark.read.option("branch", version).table(tableName).count() should be > 0L
        spark.table(tableName).count() shouldBe 0

        val refs = spark.sql(s"SELECT name FROM $tableName.refs").collect().map(_.getString(0)).toSet
        refs should contain allOf("main", version, "latest")
        refs.filter(_.startsWith("audit")) shouldBe empty
      }
    }
  }
}
