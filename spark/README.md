# radiant-open-datalake spark

This directory contains spark code used to normalize and enrich public genomics datasets.

## Requirements

- Scala 2.12.18
- Java 11

## Storage conventions

See [doc/storage_convention.md](doc/storage_convention.md)

## Developers

The dataset configuration files in `resources/config` (e.g., `qa.conf`, `staging.conf`, `prod.conf` and `test.conf`) are automatically generated from the Scala class `EtlConfiguration`.  
To (re)generate these configuration files, run the following command:

```sh
sbt "runMain org.radiant.opendatalake.config.EtlConfiguration"
```

To build a fat JAR for deployment, use:

```sh
sbt assembly
```

To run unit test, use:

```sh
sbt clean test
```

To run `spark-submit` commands locally, refer to the [sandbox](sandbox/README.md) directory. It provides a Docker Compose setup to start MinIO and an Iceberg REST catalog, along with example commands to build the JAR and launch `spark-submit`.