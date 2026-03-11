package org.radiant.opendatalake.config

import bio.ferlab.datalake.commons.config.Format.{CSV, DELTA, GFF, VCF, XML}
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config._
import bio.ferlab.datalake.commons.file.FileSystemType.S3


import pureconfig.generic.auto._

object EtlConfiguration extends App {
  val datalake_storage_id = "radiantodl_datalake"

  val qa_database = "radiantodl_qa"
  val staging_database = "radiantodl_staging"
  val prd_database = "radiantodl"

  val qa_storage = List(
    StorageConf(datalake_storage_id, "s3a://radiantodl-qa-app-datalake", S3),
  )
  val staging_storage = List(
    StorageConf(datalake_storage_id, "s3a://radiantodl-staging-app-datalake", S3),
  )
  val prd_storage = List(
    StorageConf(datalake_storage_id, "s3a://radiantodl-prd-app-datalake", S3)
  )

  val spark_conf = Map(
    // TODO: remove or adjust these confs when converting to iceberg
    "spark.sql.extensions" -> "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog" -> "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.databricks.delta.retentionDurationCheck.enabled" -> "false",
    "spark.databricks.delta.merge.repartitionBeforeWrite.enabled" -> "true",
    "spark.databricks.delta.schema.autoMerge.enabled" -> "true",
    "spark.databricks.delta.vacuum.parallelDelete.enabled" -> "true",
    "spark.delta.merge.repartitionBeforeWrite" -> "true"
  )

  val prd_spark_conf = spark_conf ++ Map(
    "hive.metastore.client.factory.class" -> "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
  )

  val gnomad_storage_id = "gnomad"
  val table_database = "radiantodl"

  def table(table_name: String): Option[TableConf] = Some(TableConf(table_database, table_name))


  val sources = List(
    //raw
    DatasetConf("raw_clinvar", datalake_storage_id, "/raw/landing/clinvar/clinvar.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_dbsnp", datalake_storage_id, "/raw/landing/dbsnp/GCF_000001405.40.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_gnomad_genomes_v3", datalake_storage_id, "/release/3.1/vcf/genomes/gnomad.genomes.v3.1.sites.chr[^M]*.vcf.bgz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")).copy(storageid = gnomad_storage_id),
    DatasetConf("raw_gnomad_joint_v4", datalake_storage_id, "/raw/landing/gnomad_v4/release/4.1/vcf/joint/gnomad.joint.v4.1.sites.chr[^M]*.vcf.bgz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_gnomad_cnv_v4", datalake_storage_id, "/raw/landing/gnomad_v4/release/4.1/exome_cnv/gnomad.v4.1.cnv.all.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_gnomad_sv_v4", datalake_storage_id, "/raw/landing/gnomad_v4/release/4.1/genome_sv/gnomad.v4.1.sv.sites.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_gnomad_constraint_v2_1_1", datalake_storage_id, "/raw/landing/gnomad_v2_1_1/gnomad.v2.1.1.lof_metrics.by_gene.txt.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_topmed_bravo", datalake_storage_id, "/raw/landing/topmed/bravo-dbsnp-*.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_1000_genomes", datalake_storage_id, "/raw/landing/1000Genomes/ALL.*.sites.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_dbnsfp", datalake_storage_id, "/raw/landing/dbNSFP/dbNSFP4.3a_variant.chr*.gz", CSV, OverWrite, readoptions = Map("sep" -> "\t", "header" -> "true", "nullValue" -> ".")),
    DatasetConf("raw_dbnsfp_annovar", datalake_storage_id, "/raw/landing/annovar/dbNSFP/hg38_dbnsfp41a.txt", CSV, OverWrite, readoptions = Map("sep" -> "\t", "header" -> "true", "nullValue" -> ".")),
    DatasetConf("raw_omim_gene_set", datalake_storage_id, "/raw/landing/omim/genemap2.txt", CSV, OverWrite, readoptions = Map("inferSchema" -> "true", "comment" -> "#", "header" -> "false", "sep" -> "\t")),
    DatasetConf("raw_orphanet_gene_association", datalake_storage_id, "/raw/landing/orphanet/en_product6.xml", XML, OverWrite),
    DatasetConf("raw_orphanet_disease_history", datalake_storage_id, "/raw/landing/orphanet/en_product9_ages.xml", XML, OverWrite),
    DatasetConf("raw_cosmic_gene_set", datalake_storage_id, "/raw/landing/cosmic/Cosmic_CancerGeneCensus_GRCh38.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_cosmic_mutation_set", datalake_storage_id, "/raw/landing/cosmic/cmc_export.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ddd_gene_set", datalake_storage_id, "/raw/landing/ddd/DDG2P.csv.gz", CSV, OverWrite, readoptions = Map("header" -> "true")),
    DatasetConf("raw_hpo_gene_set", datalake_storage_id, "/raw/landing/hpo/genes_to_phenotype.txt", CSV, OverWrite, readoptions = Map("inferSchema" -> "true", "comment" -> "#", "header" -> "false", "sep" -> "\t", "nullValue" -> "-")),
    DatasetConf("raw_refseq_human_genes", datalake_storage_id, "/raw/landing/refseq/Homo_sapiens.gene_info.gz", CSV, OverWrite, readoptions = Map("inferSchema" -> "true", "header" -> "true", "sep" -> "\t", "nullValue" -> "-")),
    DatasetConf("raw_refseq_annotation", datalake_storage_id, "/raw/landing/refseq/GCF_GRCh38_genomic.gff.gz", GFF, OverWrite),
    DatasetConf("raw_ensembl_entrez", datalake_storage_id, "/raw/landing/ensembl/Homo_sapiens.GRCh38.entrez.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ensembl_refseq", datalake_storage_id, "/raw/landing/ensembl/Homo_sapiens.GRCh38.refseq.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ensembl_uniprot", datalake_storage_id, "/raw/landing/ensembl/Homo_sapiens.GRCh38.uniprot.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ensembl_ena", datalake_storage_id, "/raw/landing/ensembl/Homo_sapiens.GRCh38.ena.tsv.gz", CSV, OverWrite, readoptions = Map("header" -> "true", "sep" -> "\t")),
    DatasetConf("raw_ensembl_gff", datalake_storage_id, "/raw/landing/ensembl/Homo_sapiens.GRCh38.gff.gz", GFF, OverWrite),
    DatasetConf("raw_spliceai_indel", datalake_storage_id, "/raw/landing/spliceai/spliceai_scores.raw.indel.hg38.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),
    DatasetConf("raw_spliceai_snv", datalake_storage_id, "/raw/landing/spliceai/spliceai_scores.raw.snv.hg38.vcf.gz", VCF, OverWrite, readoptions = Map("flattenInfoFields" -> "true", "split_multiallelics" -> "true")),

    //normalized
    DatasetConf("normalized_1000_genomes", datalake_storage_id, "/public/1000_genomes", DELTA, OverWrite, partitionby = List(), table = table("1000_genomes")),
    DatasetConf("normalized_cancer_hotspots", datalake_storage_id, "/public/cancer_hotspots", DELTA, OverWrite, partitionby = List(), table = table("cancer_hotspots")),
    DatasetConf("normalized_clinvar", datalake_storage_id, "/public/clinvar", DELTA, OverWrite, partitionby = List(), repartition = Some(Coalesce()), table = table("clinvar")),
    DatasetConf("normalized_cosmic_gene_set", datalake_storage_id, "/public/cosmic_gene_set", DELTA, OverWrite, partitionby = List(), table = table("cosmic_gene_set")),
    DatasetConf("normalized_cosmic_mutation_set", datalake_storage_id, "/public/cosmic_mutation_set", DELTA, OverWrite, partitionby = List(), table = table("cosmic_mutation_set")),
    DatasetConf("normalized_dbnsfp", datalake_storage_id, "/public/dbnsfp/variant", DELTA, OverWrite, partitionby = List("chromosome"), table = table("dbnsfp")),
    DatasetConf("normalized_dbnsfp_annovar", datalake_storage_id, "/public/annovar/dbnsfp", DELTA, OverWrite, partitionby = List("chromosome"), table = table("dbnsfp_annovar")),
    DatasetConf("normalized_dbsnp", datalake_storage_id, "/public/dbsnp", DELTA, OverWrite, partitionby = List("chromosome"), table = table("dbsnp")),
    DatasetConf("normalized_ddd_gene_set", datalake_storage_id, "/public/ddd_gene_set", DELTA, OverWrite, partitionby = List(), table = table("ddd_gene_set")),
    DatasetConf("normalized_ensembl_mapping", datalake_storage_id, "/public/ensembl_mapping", DELTA, OverWrite, partitionby = List(), table = table("ensembl_mapping"), repartition = Some(Coalesce())),
    DatasetConf("normalized_gnomad_genomes_v2_1_1", datalake_storage_id, "/public/gnomad_genomes_v2_1_1_liftover_grch38", DELTA, OverWrite, partitionby = List("chromosome"), table = table("gnomad_genomes_v2_1_1")),
    DatasetConf("normalized_gnomad_exomes_v2_1_1", datalake_storage_id, "/public/gnomad_exomes_v2_1_1_liftover_grch38", DELTA, OverWrite, partitionby = List("chromosome"), table = table("gnomad_exomes_v2_1_1")),
    DatasetConf("normalized_gnomad_constraint_v2_1_1", datalake_storage_id, "/public/gnomad_constraint_v2_1_1", DELTA, OverWrite, partitionby = List("chromosome"), table = table("gnomad_constraint_v_2_1_1")),
    DatasetConf("normalized_gnomad_genomes_v3", datalake_storage_id, "/public/gnomad_genomes_v3", DELTA, OverWrite, partitionby = List("chromosome"), table = table("gnomad_genomes_v3")),
    DatasetConf("normalized_gnomad_joint_v4", datalake_storage_id, "/public/gnomad_joint_v4", DELTA, OverWrite, partitionby = List("chromosome"), table = table("gnomad_joint_v4")),
    DatasetConf("normalized_gnomad_cnv_v4", datalake_storage_id, "/public/gnomad_cnv_v4", DELTA, OverWrite, partitionby = List("chromosome"), table = table("gnomad_cnv_v4")),
    DatasetConf("normalized_gnomad_sv_v4", datalake_storage_id, "/public/gnomad_sv_v4", DELTA, OverWrite, partitionby = List("chromosome"), table = table("gnomad_sv_v4")),
    DatasetConf("normalized_human_genes", datalake_storage_id, "/public/human_genes", DELTA, OverWrite, partitionby = List(), table = table("human_genes")),
    DatasetConf("normalized_hpo_gene_set", datalake_storage_id, "/public/hpo_gene_set", DELTA, OverWrite, partitionby = List(), table = table("hpo_gene_set")),
    DatasetConf("normalized_omim_gene_set", datalake_storage_id, "/public/omim_gene_set", DELTA, OverWrite, partitionby = List(), table = table("omim_gene_set")),
    DatasetConf("normalized_orphanet_gene_set", datalake_storage_id, "/public/orphanet_gene_set", DELTA, OverWrite, partitionby = List(), table = table("orphanet_gene_set")),
    DatasetConf("normalized_topmed_bravo", datalake_storage_id, "/public/topmed_bravo", DELTA, OverWrite, partitionby = List(), table = table("topmed_bravo")),
    DatasetConf("normalized_refseq_annotation", datalake_storage_id, "/public/refseq_annotation", DELTA, OverWrite, partitionby = List("chromosome"), table = table("refseq_annotation")),
    DatasetConf("normalized_spliceai_indel", datalake_storage_id, "/public/spliceai/indel", DELTA, OverWrite, partitionby = List("chromosome"), table = table("spliceai_indel")),
    DatasetConf("normalized_spliceai_snv", datalake_storage_id, "/public/spliceai/snv", DELTA, OverWrite, partitionby = List("chromosome"), table = table("spliceai_snv")),

    // enriched
    DatasetConf("enriched_genes", datalake_storage_id, "/public/genes", DELTA, OverWrite, partitionby = List(), table = table("genes")),
    DatasetConf("enriched_dbnsfp", datalake_storage_id, "/public/dbnsfp/scores", DELTA, OverWrite, partitionby = List("chromosome"), table = table("dbnsfp_original")),
    DatasetConf("enriched_spliceai_indel", datalake_storage_id, "/public/spliceai/enriched/indel", DELTA, OverWrite, partitionby = List("chromosome"), repartition = Some(RepartitionByRange(columnNames = Seq("chromosome", "start"))), table = table("spliceai_enriched_indel")),
    DatasetConf("enriched_spliceai_snv", datalake_storage_id, "/public/spliceai/enriched/snv", DELTA, OverWrite, partitionby = List("chromosome"), repartition = Some(RepartitionByRange(columnNames = Seq("chromosome", "start"))), table = table("spliceai_enriched_snv")),
    DatasetConf("enriched_rare_variant", datalake_storage_id, "/public/rare_variant/enriched", DELTA, OverWrite, partitionby = List("chromosome", "is_rare"), table = table("rare_variant_enriched"))
  )

  val qa_conf = SimpleConfiguration(DatalakeConf(
    storages = qa_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(qa_database, t.name)))),
    sparkconf = spark_conf
  ))

  val staging_conf = SimpleConfiguration(DatalakeConf(
    storages = staging_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(staging_database, t.name)))),
    sparkconf = spark_conf
  ))

  val prd_conf = SimpleConfiguration(DatalakeConf(
    storages = prd_storage,
    sources = sources.map(ds => ds.copy(table = ds.table.map(t => TableConf(prd_database, t.name)))),
    sparkconf = prd_spark_conf
  ))

  val test_conf = SimpleConfiguration(DatalakeConf(
    storages = List(),
    sources = sources,
    sparkconf = spark_conf
  ))

  ConfigurationWriter.writeTo("src/main/resources/config/qa.conf", qa_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/staging.conf", staging_conf)
  ConfigurationWriter.writeTo("src/main/resources/config/prod.conf", prd_conf)
  ConfigurationWriter.writeTo("src/test/resources/config/test.conf", test_conf)
}
