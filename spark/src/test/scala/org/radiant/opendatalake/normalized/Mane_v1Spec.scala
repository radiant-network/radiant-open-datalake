package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.DatasetConf
import bio.ferlab.datalake.testutils.TestETLContext
import org.radiant.opendatalake.testutils.{CleanUpBeforeAll, CreateDatabasesBeforeAll, SparkSpec}

case class NormalizedMane(ncbi_gene_id: Option[Int] = Some(1),
                          ensembl_gene_id: Option[String] = Some("ENSG00000121410"),
                          ensembl_gene_version: Option[Int] = Some(13),
                          hgnc_id: Option[String] = Some("HGNC:5"),
                          symbol: Option[String] = Some("A1BG"),
                          name: Option[String] = Some("alpha-1-B glycoprotein"),
                          refseq_transcript_id: Option[String] = Some("NM_130786.4"),
                          refseq_transcript_accession: Option[String] = Some("NM_130786"),
                          refseq_transcript_version: Option[Int] = Some(4),
                          refseq_protein_id: Option[String] = Some("NP_570602.2"),
                          ensembl_transcript_id: Option[String] = Some("ENST00000263100"),
                          ensembl_transcript_version: Option[Int] = Some(8),
                          ensembl_protein_id: Option[String] = Some("ENSP00000263100.2"),
                          mane_status: Option[String] = Some("MANE Select"),
                          chromosome_accession: Option[String] = Some("NC_000019.10"),
                          start: Option[Long] = Some(58345183L),
                          end: Option[Long] = Some(58353492L),
                          strand: Option[String] = Some("-"))

class Mane_v1Spec extends SparkSpec with CreateDatabasesBeforeAll with CleanUpBeforeAll {

  import spark.implicits._

  private val source: DatasetConf = conf.getDataset("raw_mane_summary")

  private def job = Mane_v1(TestETLContext(), version = "test", rawStorage = "", tablePrefix = "mane")

  private val destination: DatasetConf = job.mainDestination
  assert(destination.table.map(_.name).contains("mane_v1"), s"MAJOR 1 must publish to mane_v1, not ${destination.table}")

  override val dbToCreate: List[String] = List(destination.table.map(_.database).get)
  override val dsToClean: List[DatasetConf] = List(destination)

  // The header of MANE.GRCh38.v1.4.summary.txt.gz, read as is (header=true keeps the leading '#').
  private val rawColumns = Seq("#NCBI_GeneID", "Ensembl_Gene", "HGNC_ID", "symbol", "name", "RefSeq_nuc", "RefSeq_prot",
    "Ensembl_nuc", "Ensembl_prot", "MANE_status", "GRCh38_chr", "chr_start", "chr_end", "chr_strand")

  "transform" should "split the Ensembl and RefSeq transcript ids, keep the RefSeq ids whole and keep both MANE statuses" in {
    val raw = Seq(
      ("GeneID:1", "ENSG00000121410.13", "HGNC:5", "A1BG", "alpha-1-B glycoprotein", "NM_130786.4", "NP_570602.2",
        "ENST00000263100.8", "ENSP00000263100.2", "MANE Select", "NC_000019.10", "58345183", "58353492", "-"),
      ("GeneID:2", "ENSG00000000002.4", "HGNC:2", "GENE2", "gene two", "NM_000002.3", "NP_000002.1",
        "ENST00000000002.7", "ENSP00000000002.5", "MANE Plus Clinical", "NC_000001.11", "100", "200", "+")
    ).toDF(rawColumns: _*)

    val result = job.transformSingle(Map(source.id -> raw))

    result.as[NormalizedMane].collect() should contain theSameElementsAs expectedRows
  }

  // The fixture holds the same two rows, so the path template, `sep`, the '#' header and the gzip are read as on EMR.
  "extract" should "read the gzipped summary at its versioned raw path" in {
    val fromFile = Mane_v1(TestETLContext(), version = "1.4", rawStorage = getClass.getResource("/raw/landing").getPath, tablePrefix = "mane")

    fromFile.transformSingle(fromFile.extract()).as[NormalizedMane].collect() should contain theSameElementsAs expectedRows
  }

  private lazy val expectedRows = Seq(
    NormalizedMane(),
    NormalizedMane(
      ncbi_gene_id = Some(2),
      ensembl_gene_id = Some("ENSG00000000002"),
      ensembl_gene_version = Some(4),
      hgnc_id = Some("HGNC:2"),
      symbol = Some("GENE2"),
      name = Some("gene two"),
      refseq_transcript_id = Some("NM_000002.3"),
      refseq_transcript_accession = Some("NM_000002"),
      refseq_transcript_version = Some(3),
      refseq_protein_id = Some("NP_000002.1"),
      ensembl_transcript_id = Some("ENST00000000002"),
      ensembl_transcript_version = Some(7),
      ensembl_protein_id = Some("ENSP00000000002.5"),
      mane_status = Some("MANE Plus Clinical"),
      chromosome_accession = Some("NC_000001.11"),
      start = Some(100L),
      end = Some(200L),
      strand = Some("+")
    )
  )

  private val tableName: String = destination.table.map(_.fullName).get
  private def onBranch(branch: String) = spark.read.option("branch", branch).table(tableName).as[NormalizedMane]

  "load" should "publish the version to its own branch and leave main empty" in {
    val firstLoad = Seq(NormalizedMane(symbol = Some("GENE1")))
    val secondLoad = Seq(NormalizedMane(symbol = Some("GENE1")), NormalizedMane(symbol = Some("GENE2")))

    job.loadSingle(firstLoad.toDF())
    onBranch("test").collect() should contain theSameElementsAs firstLoad

    job.loadSingle(secondLoad.toDF())
    onBranch("test").collect() should contain theSameElementsAs secondLoad

    withClue("main must stay empty — consumers read the dataset_version branch: ") {
      spark.table(tableName).count() shouldBe 0
    }
  }
}
