# radiant-open-datalake spark

This directory contains spark code used to normalize and enrich public genomics datasets.

## Requirements

- Scala 2.12.18
- Java 11

## Storage conventions

See [doc/storage_convention.md](doc/storage_convention.md)

## Developers

The dataset configuration files are generated from the Scala class `EtlConfiguration`:

| File                                  | Used by                                                                                          |
|---------------------------------------|--------------------------------------------------------------------------------------------------|
| `src/main/resources/config/prd.conf`  | the deployed job, and shipped inside the fat JAR                                                 |
| `src/test/resources/config/test.conf` | the test suite, with the Iceberg paths rewritten to `/<table-name>` for the local Hadoop catalog |

**Never hand-edit either file**, but instead edit `EtlConfiguration.scala` and regenerate, or the next run silently reverts the change:

```sh
sbt "runMain org.radiant.opendatalake.config.EtlConfiguration"
```

The job is launched with `--config config/<ENV>.conf`, so an environment other than `prd` (`qa`, `staging`, …)
needs its `StorageConf` list and its own `ConfigurationWriter.writeTo` line added to `EtlConfiguration`
first.

To build a fat JAR for deployment, use:

```sh
sbt assembly
```

To run unit test, use:

```sh
sbt clean test
```

To run `spark-submit` commands locally, refer to the [sandbox](sandbox/README.md) directory. It provides a Docker Compose setup to start MinIO and an Iceberg REST catalog, along with example commands to build the JAR and launch `spark-submit`.