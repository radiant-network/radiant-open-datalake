package org.radiant.opendatalake.config

import bio.ferlab.datalake.commons.config.Format.{BINARY, CSV, GFF, ICEBERG, VCF, XML}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3


import pureconfig.generic.auto._

object EtlConfiguration extends App {
  val raw_storage_id = "raw_storage"
  val iceberg_storage_id = "iceberg_storage"
  val iceberg_database = "reference"

  /* ********************************************************************************************
   * Note:                                                                                      *
   *                                                                                            *
   * When creating the catalog with the REST Iceberg management service, the catalog root path. *
   * must be configured as s3a://opendatalake-<env>/iceberg.                                    *
   *                                                                                            *
   * All Iceberg databases (namespaces) will be created as subfolders under this catalog root.  *
   * For example, with database = "reference", the storage prefix for tables in this database   *
   * will be:                                                                                   *
   *   s3://opendatalake-<env>/iceberg/reference/                                               *
                                                                                                *
   **********************************************************************************************/
  val prd_storage = List(
    StorageConf(iceberg_storage_id, s"s3a://opendatalake-prd/iceberg/${iceberg_database}", S3),
    StorageConf(raw_storage_id, "s3a://opendatalake-prd/raw/landing", S3)
  )

  /*
    Environment-dependent or sensitive configuration properties are intentionally excluded here.
    We assume these will be injected during the deployment process or at runtime.

    It may be relevant to add certain Glue-specific properties to the qa, staging, and prod configuration
    files in the future.  For now, we are omitting them since we cannot properly test them. If this
    situation changes, consider including them here.
  */
  val spark_conf = Map(
    "spark.sql.extensions" -> "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.opendatalake" -> "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.defaultCatalog" -> "opendatalake"
  )

  def table(table_name: String): Option[TableConf] = Some(TableConf(iceberg_database, table_name))

  private lazy val contracts: Contracts = Contracts.load()

  // This is useful for customizing the DatasetConf with the information coming from `contracts.yml`
  def buildNormalizedDatasetConf(source: String,
                                 partitionby: List[String] = List(),
                                 repartition: Option[Repartition] = None): DatasetConf = {
    val prefix = contracts.tablePrefixOf(source).filter(_.nonEmpty).getOrElse(
      throw new IllegalArgumentException(s"source '$source' declares no table_prefix in contracts.yml")
    )

    DatasetConf(
      id = s"normalized_$source",
      storageid = iceberg_storage_id,
      path = s"/normalized/$prefix",
      format = ICEBERG,
      loadtype = OverWrite,
      table = table(prefix),
      partitionby = partitionby,
      repartition = repartition
    )
  }

  val sources = List(
    //raw
    DatasetConf("raw_clinvar", raw_storage_id, "/clinvar/{{VERSION}}/*.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true")),
    DatasetConf("raw_dbsnp", raw_storage_id, "/dbsnp/{{VERSION}}/*.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true")),
    DatasetConf("raw_gnomad_joint", raw_storage_id, "/gnomad_joint/{{VERSION}}/*.vcf.bgz",  VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true")),
    DatasetConf("raw_gnomad_cnv", raw_storage_id, "/gnomad_cnv/{{VERSION}}/*.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true")),
    DatasetConf("raw_gnomad_sv", raw_storage_id, "/gnomad_sv/{{VERSION}}/*.sv.sites.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true")),
    DatasetConf("raw_gnomad_constraint_v2_1_1", raw_storage_id, "/gnomad_v2_1_1/gnomad.v2.1.1.lof_metrics.by_gene.txt.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_topmed_bravo", raw_storage_id, "/topmed/bravo-dbsnp-*.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_1000_genomes", raw_storage_id, "/1000_genomes/{{VERSION}}/*.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_dbnsfp", raw_storage_id, "/dbNSFP/dbNSFP4.3a_variant.chr*.gz", CSV, OverWrite, readoptions = Map("sep" -> "\t", "header" -> "true", "nullValue" -> ".")),
    DatasetConf("raw_dbnsfp_annovar", raw_storage_id, "/annovar/dbNSFP/hg38_dbnsfp41a.txt", CSV, OverWrite, readoptions = Map("sep" -> "\t", "header" -> "true", "nullValue" -> ".")),
    DatasetConf("raw_omim_gene_set", raw_storage_id, "/omim/genemap2.txt", CSV, OverWrite, readoptions = Map("inferSchema" -> "true", "comment" -> "#", "header" -> "false", "sep" -> "\t")),
    DatasetConf("raw_orphanet_gene_association", raw_storage_id, "/orphanet/en_product6.xml", XML, OverWrite),
    DatasetConf("raw_orphanet_disease_history", raw_storage_id, "/orphanet/en_product9_ages.xml", XML, OverWrite),
    DatasetConf("raw_cosmic_gene_set", raw_storage_id, "/cosmic/Cosmic_CancerGeneCensus_GRCh38.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_cosmic_mutation_set", raw_storage_id, "/cosmic/cmc_export.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ddd_gene_set", raw_storage_id, "/ddd/DDG2P.csv.gz", CSV, OverWrite, readoptions = Map("header" -> "true")),
    DatasetConf("raw_hpo_gene_set", raw_storage_id, "/hpo/genes_to_phenotype.txt", CSV, OverWrite, readoptions = Map("inferSchema" -> "true", "comment" -> "#", "header" -> "false", "sep" -> "\t", "nullValue" -> "-")),
    DatasetConf("raw_hpo_terms", raw_storage_id, "/hpo_terms/{{VERSION}}/*.obo", BINARY, OverWrite),
    DatasetConf("raw_mondo", raw_storage_id, "/mondo/{{VERSION}}/*.obo", BINARY, OverWrite),
    DatasetConf("raw_refseq_human_genes", raw_storage_id, "/refseq/Homo_sapiens.gene_info.gz", CSV, OverWrite, readoptions = Map("inferSchema" -> "true", "header" -> "true", "sep" -> "\t", "nullValue" -> "-")),
    DatasetConf("raw_refseq_annotation", raw_storage_id, "/refseq/GCF_GRCh38_genomic.gff.gz", GFF, OverWrite),
    DatasetConf("raw_ensembl_entrez", raw_storage_id, "/ensembl/Homo_sapiens.GRCh38.entrez.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ensembl_refseq", raw_storage_id, "/ensembl/Homo_sapiens.GRCh38.refseq.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ensembl_uniprot", raw_storage_id, "/ensembl/Homo_sapiens.GRCh38.uniprot.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ensembl_ena", raw_storage_id, "/ensembl/Homo_sapiens.GRCh38.ena.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ensembl_gff", raw_storage_id, "/ensembl/Homo_sapiens.GRCh38.gff.gz", GFF, OverWrite),
    DatasetConf("raw_spliceai_indel", raw_storage_id, "/spliceai/spliceai_scores.raw.indel.hg38.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_spliceai_snv", raw_storage_id, "/spliceai/spliceai_scores.raw.snv.hg38.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),

    //normalized
    buildNormalizedDatasetConf("1000_genomes", partitionby = List("chromosome")),
    DatasetConf("normalized_cancer_hotspots", iceberg_storage_id, "/normalized/cancer_hotspots", ICEBERG, OverWrite, partitionby = List(), table = table("cancer_hotspots")),
    buildNormalizedDatasetConf("clinvar", repartition = Some(Coalesce())),
    DatasetConf("normalized_cosmic_gene_set", iceberg_storage_id, "/normalized/cosmic_gene_set", ICEBERG, OverWrite, partitionby = List(), table = table("cosmic_gene_set")),
    DatasetConf("normalized_cosmic_mutation_set", iceberg_storage_id, "/normalized/cosmic_mutation_set", ICEBERG, OverWrite, partitionby = List(), table = table("cosmic_mutation_set")),
    DatasetConf("normalized_dbnsfp", iceberg_storage_id, "/normalized/dbnsfp/variant", ICEBERG, OverWrite, partitionby = List("chromosome"), table = table("dbnsfp")),
    DatasetConf("normalized_dbnsfp_annovar", iceberg_storage_id, "/normalized/annovar/dbnsfp", ICEBERG, OverWrite, partitionby = List("chromosome"), table = table("dbnsfp_annovar")),
    buildNormalizedDatasetConf("dbsnp", partitionby = List("chromosome")),
    DatasetConf("normalized_ddd_gene_set", iceberg_storage_id, "/normalized/ddd_gene_set", ICEBERG, OverWrite, partitionby = List(), table = table("ddd_gene_set")),
    DatasetConf("normalized_ensembl_mapping", iceberg_storage_id, "/normalized/ensembl_mapping", ICEBERG, OverWrite, partitionby = List(), table = table("ensembl_mapping"), repartition = Some(Coalesce())),
    DatasetConf("normalized_gnomad_constraint_v2_1_1", iceberg_storage_id, "/normalized/gnomad_constraint_v2_1_1", ICEBERG, OverWrite, partitionby = List("chromosome"), table = table("gnomad_constraint_v_2_1_1")),
    buildNormalizedDatasetConf("gnomad_cnv", partitionby = List("chromosome")),
    buildNormalizedDatasetConf("gnomad_joint", partitionby = List("chromosome")),
    buildNormalizedDatasetConf("gnomad_sv", partitionby = List("chromosome")),
    DatasetConf("normalized_human_genes", iceberg_storage_id, "/normalized/human_genes", ICEBERG, OverWrite, partitionby = List(), table = table("human_genes")),
    DatasetConf("normalized_hpo_gene_set", iceberg_storage_id, "/normalized/hpo_gene_set", ICEBERG, OverWrite, partitionby = List(), table = table("hpo_gene_set")),
    buildNormalizedDatasetConf("hpo_terms"),
    buildNormalizedDatasetConf("mondo"),
    DatasetConf("normalized_omim_gene_set", iceberg_storage_id, "/normalized/omim_gene_set", ICEBERG, OverWrite, partitionby = List(), table = table("omim_gene_set")),
    DatasetConf("normalized_orphanet_gene_set", iceberg_storage_id, "/normalized/orphanet_gene_set", ICEBERG, OverWrite, partitionby = List(), table = table("orphanet_gene_set")),
    DatasetConf("normalized_topmed_bravo", iceberg_storage_id, "/normalized/topmed_bravo", ICEBERG, OverWrite, partitionby = List(), table = table("topmed_bravo")),
    DatasetConf("normalized_refseq_annotation", iceberg_storage_id, "/normalized/refseq_annotation", ICEBERG, OverWrite, partitionby = List("chromosome"), table = table("refseq_annotation")),
    DatasetConf("normalized_spliceai_indel", iceberg_storage_id, "/normalized/spliceai/indel", ICEBERG, OverWrite, partitionby = List("chromosome"), table = table("spliceai_indel")),
    DatasetConf("normalized_spliceai_snv", iceberg_storage_id, "/normalized/spliceai/snv", ICEBERG, OverWrite, partitionby = List("chromosome"), table = table("spliceai_snv")),

    // enriched
    DatasetConf("enriched_genes", iceberg_storage_id, "/enriched/genes", ICEBERG, OverWrite, partitionby = List(), table = table("genes")),
    DatasetConf("enriched_dbnsfp", iceberg_storage_id, "/enriched/dbnsfp/scores", ICEBERG, OverWrite, partitionby = List("chromosome"), table = table("dbnsfp_original")),
    DatasetConf("enriched_spliceai_indel", iceberg_storage_id, "/enriched/spliceai/indel", ICEBERG, OverWrite, partitionby = List("chromosome"), repartition = Some(RepartitionByRange(columnNames = Seq("chromosome", "start"))), table = table("spliceai_enriched_indel")),
    DatasetConf("enriched_spliceai_snv", iceberg_storage_id, "/enriched/spliceai/snv", ICEBERG, OverWrite, partitionby = List("chromosome"), repartition = Some(RepartitionByRange(columnNames = Seq("chromosome", "start"))), table = table("spliceai_enriched_snv"))
  )

  val prd_conf = SimpleConfiguration(DatalakeConf(
    storages = prd_storage,
    sources = sources,
    sparkconf = spark_conf
  ))

  val test_conf = SimpleConfiguration(DatalakeConf(
    storages = List(),
    /*
      Modifying sources paths for compatibility with the Hadoop catalog used in local testing.
      This catalog requires that table paths exactly match the default location it would assign, i.e.
      "custom" locations are not supported. */
    sources = sources.map {
      case ds if ds.storageid == iceberg_storage_id => ds.copy(path = "/" + ds.table.get.name)
      case ds => ds
    },
    sparkconf = spark_conf
  ))

  ConfigurationWriter.writeTo("src/main/resources/config/prd.conf", prd_conf)
  ConfigurationWriter.writeTo("src/test/resources/config/test.conf", test_conf)
}
