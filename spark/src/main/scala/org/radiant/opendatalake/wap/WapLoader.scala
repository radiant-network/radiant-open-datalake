package org.radiant.opendatalake.wap

import bio.ferlab.datalake.commons.config.DatasetConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object WapLoader {

  private val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  private val MainBranch: String = "main"

  /*
    Ref names reach SQL as identifiers, and neither source's version is a bare word: clinvar's is
    all-digits ("20260715"), dbsnp's contains a dot ("GCF_000001405.40"). Iceberg itself does not
    validate ref names, they are plain keys in table metadata 
  */
  private def quoted(ref: String): String = s"`$ref`"

  def auditBranchOf(version: String): String = s"audit_$version"

  /*
    Iceberg's SparkCatalog caches table metadata with a 30s default TTL, and every step here commits to the
    table and then immediately reads the ref it just moved. The REFRESH keeps those reads honest.
  */
  private def snapshotIdOf(tableName: String, ref: String)(implicit spark: SparkSession): Option[Long] = {
    spark.sql(s"REFRESH TABLE $tableName")
    spark
      .sql(s"SELECT snapshot_id FROM $tableName.refs WHERE name = '$ref'")
      .collect()
      .headOption
      .map(_.getLong(0))
  }

  /**
   * Publishes `df` to the `version` branch of `dest`'s Iceberg table, leaving `main` empty.
   *
   * @return the published data, read back from the version branch.
   */
  def publish(destination: DatasetConf, data: DataFrame, version: String)(implicit spark: SparkSession): DataFrame = {
    val table = destination.table.getOrElse(
      throw new IllegalArgumentException(
        s"${destination.id} declares no table; a WAP destination must be an Iceberg table"
      )
    )
    val tableName = s"${table.database}.${table.name}" // resolves through spark.sql.defaultCatalog=opendatalake
    val auditBranch = auditBranchOf(version)

    // loadDataset used to do this for us; the bypass has to keep it.
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${table.database}")
    ensureEmptyMain(tableName, data, destination.partitionby)

    val mainSnapshotId = snapshotIdOf(tableName, MainBranch).getOrElse(
      throw new IllegalStateException(s"$tableName has no '$MainBranch' ref after bootstrap")
    )

    /*
      CREATE OR REPLACE, not CREATE: a retry after a crashed run must start from empty main rather than
      append onto whatever the previous attempt left on the audit branch.
    */
    log.info(s"WAP $tableName: staging version '$version' on branch '$auditBranch' from $MainBranch@$mainSnapshotId")
    spark.sql(s"ALTER TABLE $tableName CREATE OR REPLACE BRANCH ${quoted(auditBranch)} AS OF VERSION $mainSnapshotId")

    /*
      The write MUST target the branch-qualified identifier. `writeTo(tableName).option("branch", auditBranch)`
      is silently ignored on the write path — it commits to `main` instead, which both defeats the pattern and
      leaves the version branch pointing one write behind. The `branch` option does work for reads.
    */
    val auditIdentifier = s"$tableName.${quoted(s"branch_$auditBranch")}"
    data.writeTo(auditIdentifier).append()

    val auditSnapshotId = snapshotIdOf(tableName, auditBranch).getOrElse(
      throw new IllegalStateException(s"audit branch '$auditBranch' vanished mid-run on $tableName")
    )

    // CREATE OR REPLACE again: re-importing a dataset_version overwrites it (SJRA-1546 §3.4, "Data Upgrades").
    log.info(s"WAP $tableName: publishing '$version' from '$auditBranch'@$auditSnapshotId, dropping '$auditBranch'")
    spark.sql(s"ALTER TABLE $tableName CREATE OR REPLACE BRANCH ${quoted(version)} AS OF VERSION $auditSnapshotId")
    spark.sql(s"ALTER TABLE $tableName DROP BRANCH ${quoted(auditBranch)}")

    spark.sql(s"REFRESH TABLE $tableName")
    spark.read.option("branch", version).table(tableName)
  }

  /*
    First run has nothing to branch from: the table does not exist. Create it from the DataFrame's schema
    with no rows. The zero-row create still commits an (empty) append snapshot, which is what CREATE BRANCH
    needs to point at — so main ends up existing, snapshotted, and empty. If that ever stops holding, the
    `base` lookup in `publish` fails loudly rather than writing to the wrong ref.
  */
  private def ensureEmptyMain(tableName: String, data: DataFrame, partitionBy: List[String])
                             (implicit spark: SparkSession): Unit =
    if (!spark.catalog.tableExists(tableName)) {
      log.info(s"WAP $tableName: table absent, creating it empty on $MainBranch")
      val writer = data.limit(0).writeTo(tableName)
      val partitioned = partitionBy match {
        case Nil          => writer
        case head :: rest => writer.partitionedBy(col(head), rest.map(col): _*)
      }
      partitioned.create()
    }
}
