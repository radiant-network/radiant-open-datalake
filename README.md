# radiant-open-datalake

**radiant-open-datalake** centralizes and automates the ingestion, management, and versioning of public third-party datasets (e.g., ClinVar, Ensembl, gnomAD) to be reused across all Radiant deployments and related projects.

## Overview

The project is organized as a monorepo:

- [**airflow**](airflow/README.md): Contains airflow code for orchestrating data ingestion, processing and publication workflows.
- [**spark**](spark/README.md): Contains Spark jobs for data transformation and normalization.

## Data sources

The following sources are currently included in the available data sources:

### Automatic

New versions are discovered and imported automatically.

| Source | Table | Release notes |
|--------|-------|---------------|
| [NCBI ClinVar](https://www.ncbi.nlm.nih.gov/clinvar/) | `clinvar_v1` | [v1](spark/doc/release-notes/clinvar/v1.md) |
| [NCBI dbSNP](https://www.ncbi.nlm.nih.gov/snp/) | `dbsnp_v1` | [v1](spark/doc/release-notes/dbsnp/v1.md) |
| [Mondo Disease Ontology](https://mondo.monarchinitiative.org/) | `mondo_v1` | [v1](spark/doc/release-notes/mondo/v1.md) |
| [Human Phenotype Ontology (Terms)](https://hpo.jax.org/) | `hpo_terms_v1` | [v1](spark/doc/release-notes/hpo_terms/v1.md) |
| [Human Phenotype Ontology (Genes)](https://hpo.jax.org/) | `hpo_genes_v1` | [v1](spark/doc/release-notes/hpo_genes/v1.md) |

### Manual

Imported on demand.

| Source | Table | Release notes |
|--------|-------|---------------|
| [1000 Genomes Project](https://www.internationalgenome.org/home) | `1000_genomes_v1` | [v1](spark/doc/release-notes/1000_genomes/v1.md) |
| [gnomAD Joint Frequency](https://gnomad.broadinstitute.org/) | `gnomad_joint_v1` | [v1](spark/doc/release-notes/gnomad_joint/v1.md) |
| [gnomAD Exome CNV](https://gnomad.broadinstitute.org/) | `gnomad_cnv_v1` | [v1](spark/doc/release-notes/gnomad_cnv/v1.md) |
| [gnomAD Structural Variants](https://gnomad.broadinstitute.org/data#v4-structural-variants) | `gnomad_sv_v1` | [v1](spark/doc/release-notes/gnomad_sv/v1.md) |
| [SpliceAI](https://github.com/Illumina/SpliceAI) | `spliceai_v1` | [v1](spark/doc/release-notes/spliceai/v1.md) |

## Architecture diagram

![Architecture diagram](doc/images/public_datalake_architecture_graph.png)

## Links

[Chosen architecture ADR](https://github.com/radiant-network/architecture/blob/main/decisions/0010-public-datalake-architecture.md)
