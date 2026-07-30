package org.radiant.opendatalake.normalized.io

import bio.ferlab.datalake.commons.config.DatasetConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/*
  Write-Audit-Publish for a versioned Iceberg table (SJRA-1546 §2.1).

  `main` is kept permanently empty and serves only as the clean base to branch from; the published data
  lives on a branch named exactly the dataset_version. Per run: reset a transient `audit_{version}`
  branch to main, write there, cut the permanent branch from audit's resulting snapshot, drop audit.

  This deliberately bypasses the FerLab load path. `ETL.loadDataset` ends at DataFrameWriter V1
  (`df.write.mode(Overwrite).saveAsTable(...)`), which Spark resolves to ReplaceTableAsSelect — a staged
  *table replace*. That is the one operation that would discard the refs this pattern is built on, and it
  is also why the `spark.wap.branch` session conf is not an option here: Iceberg honours it for
  append/overwrite-by-expression writes, not for a staged replace.
*/
object WapLoader {

  private val log: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  val MainBranch: String = "main"

  /*
    Ref names reach SQL as identifiers, and neither source's version is a bare word: clinvar's is
    all-digits ("20260715"), dbsnp's contains a dot ("GCF_000001405.40"). Iceberg itself does not
    validate ref names — they are plain keys in table metadata — so quoting is the whole problem.
  */
  private def quoted(ref: String): String = s"`$ref`"

  def auditBranch(version: String): String = s"audit_$version"

  /*
    Iceberg's SparkCatalog caches table metadata with a 30s default TTL, and every step here commits to the
    table and then immediately reads the ref it just moved. The REFRESH keeps those reads honest.
  */
  private def snapshotIdOf(fqn: String, ref: String)(implicit spark: SparkSession): Option[Long] = {
    spark.sql(s"REFRESH TABLE $fqn")
    spark
      .sql(s"SELECT snapshot_id FROM $fqn.refs WHERE name = '$ref'")
      .collect()
      .headOption
      .map(_.getLong(0))
  }

  /**
   * Publishes `df` to the `version` branch of `dest`'s Iceberg table, leaving `main` empty.
   *
   * @return the published data, read back from the version branch.
   */
  def publish(dest: DatasetConf, df: DataFrame, version: String)(implicit spark: SparkSession): DataFrame = {
    val table = dest.table.getOrElse(
      throw new IllegalArgumentException(s"${dest.id} declares no table; a WAP destination must be an Iceberg table")
    )
    val fqn = s"${table.database}.${table.name}" // resolves through spark.sql.defaultCatalog=opendatalake
    val audit = auditBranch(version)

    // loadDataset used to do this for us; the bypass has to keep it.
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${table.database}")
    ensureEmptyMain(fqn, df, dest)

    val base = snapshotIdOf(fqn, MainBranch).getOrElse(
      throw new IllegalStateException(s"$fqn has no '$MainBranch' ref after bootstrap")
    )

    /*
      CREATE OR REPLACE, not CREATE: a retry after a crashed run must start from empty main rather than
      append onto whatever the previous attempt left on the audit branch.
    */
    log.info(s"WAP $fqn: staging version '$version' on branch '$audit' from $MainBranch@$base")
    spark.sql(s"ALTER TABLE $fqn CREATE OR REPLACE BRANCH ${quoted(audit)} AS OF VERSION $base")

    /*
      The write MUST target the branch-qualified identifier. `writeTo(fqn).option("branch", audit)` is
      silently ignored on the write path — it commits to `main` instead, which both defeats the pattern and
      leaves the version branch pointing one write behind. The `branch` option does work for reads.
    */
    df.writeTo(s"$fqn.${quoted(s"branch_$audit")}").append()

    val audited = snapshotIdOf(fqn, audit).getOrElse(
      throw new IllegalStateException(s"audit branch '$audit' vanished mid-run on $fqn")
    )

    // CREATE OR REPLACE again: re-importing a dataset_version overwrites it (SJRA-1546 §3.4, "Data Upgrades").
    log.info(s"WAP $fqn: publishing branch '$version' from '$audit'@$audited, dropping '$audit'")
    spark.sql(s"ALTER TABLE $fqn CREATE OR REPLACE BRANCH ${quoted(version)} AS OF VERSION $audited")
    spark.sql(s"ALTER TABLE $fqn DROP BRANCH ${quoted(audit)}")

    spark.sql(s"REFRESH TABLE $fqn")
    spark.read.option("branch", version).table(fqn)
  }

  /*
    First run has nothing to branch from: the table does not exist. Create it from the DataFrame's schema
    with no rows. The zero-row create still commits an (empty) append snapshot, which is what CREATE BRANCH
    needs to point at — so main ends up existing, snapshotted, and empty. If that ever stops holding, the
    `base` lookup in `publish` fails loudly rather than writing to the wrong ref.
  */
  private def ensureEmptyMain(fqn: String, df: DataFrame, dest: DatasetConf)(implicit spark: SparkSession): Unit =
    if (!spark.catalog.tableExists(fqn)) {
      log.info(s"WAP $fqn: table absent, creating it empty on $MainBranch")
      val writer = df.limit(0).writeTo(fqn)
      (dest.partitionby match {
        case Nil          => writer
        case head :: rest => writer.partitionedBy(col(head), rest.map(col): _*)
      }).create()
    }
}
