package org.radiant.opendatalake.wap.iceberg

import bio.ferlab.datalake.commons.config.TableConf
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{DataFrame, SparkSession}

object IcebergTable {

  val MainBranch: String = "main"

  // Necessary to perform schema merge on writes: https://iceberg.apache.org/docs/latest/spark-writes/#schema-merge
  private[iceberg] val AcceptAnySchemaProperty: String = "write.spark.accept-any-schema"
  private[iceberg] val MergeSchemaOption: String = "merge-schema"

  // TableCatalog.PROP_LOCATION — what Iceberg's SparkCatalog reads to place a table it is creating.
  private[iceberg] val LocationProperty: String = "location"

  def apply(table: TableConf): IcebergTable = IcebergTable(table.database, table.name)
}


case class IcebergTable(database: String, name: String) {

  import IcebergTable.{AcceptAnySchemaProperty, LocationProperty, MainBranch, MergeSchemaOption}

  val fullName: String = s"$database.$name"

  // Necessary for dataset versions containing only digits (e.g.: 20260728 for Clinvar).
  // This prevents them from being interpreted as numbers.
  private def quoted(ref: String): String = s"`$ref`"

  private def branchIdentifier(branch: String): String = s"$fullName.${quoted(s"branch_$branch")}"

  private def refresh()(implicit spark: SparkSession): Unit = spark.sql(s"REFRESH TABLE $fullName")

  def exists()(implicit spark: SparkSession): Boolean = spark.catalog.tableExists(fullName)

  def rowCount()(implicit spark: SparkSession): Long = spark.table(fullName).count()

  def snapshotIdOf(ref: String)(implicit spark: SparkSession): Option[Long] = {
    refresh()
    spark
      .sql(s"SELECT snapshot_id FROM $fullName.refs WHERE name = '$ref'")
      .collect()
      .headOption
      .map(_.getLong(0))
  }

  def mainSnapshotId()(implicit spark: SparkSession): Option[Long] = snapshotIdOf(MainBranch)

  def createOrReplaceBranch(branch: String, atSnapshotId: Long)(implicit spark: SparkSession): Unit =
    spark.sql(s"ALTER TABLE $fullName CREATE OR REPLACE BRANCH ${quoted(branch)} AS OF VERSION $atSnapshotId")

  def dropBranch(branch: String)(implicit spark: SparkSession): Unit =
    spark.sql(s"ALTER TABLE $fullName DROP BRANCH ${quoted(branch)}")

  def overwriteBranch(branch: String, data: DataFrame): Unit =
    data.writeTo(branchIdentifier(branch)).option(MergeSchemaOption, "true").overwrite(lit(true))

  def schemaEvolutionEnabled()(implicit spark: SparkSession): Boolean =
    spark
      .sql(s"SHOW TBLPROPERTIES $fullName")
      .where(col("key") === AcceptAnySchemaProperty)
      .collect()
      .exists(_.getString(1).equalsIgnoreCase("true"))

  def enableSchemaEvolution()(implicit spark: SparkSession): Unit =
    spark.sql(s"ALTER TABLE $fullName SET TBLPROPERTIES ('$AcceptAnySchemaProperty' = 'true')")

  def readBranch(branch: String)(implicit spark: SparkSession): DataFrame = {
    refresh()
    spark.read.option("branch", branch).table(fullName)
  }

  def deleteAll()(implicit spark: SparkSession): Unit = spark.sql(s"DELETE FROM $fullName")

  /*
    `location` is the dataset's declared location; Iceberg reads it off the create properties as
    TableCatalog.PROP_LOCATION. Without it the catalog picks its own default (Glue: <warehouse>/<db>.db/<table>),
    which is not where EtlConfiguration says the dataset lives — and the FerLab load path this replaced did
    pass it, as `.option("path", location).saveAsTable(...)`.
  */
  def createEmpty(schemaSource: DataFrame, partitionBy: List[String], location: String): Unit = {
    val writer = schemaSource
      .limit(0)
      .writeTo(fullName)
      .tableProperty(AcceptAnySchemaProperty, "true")
      .tableProperty(LocationProperty, location)
    val partitioned = partitionBy match {
      case Nil          => writer
      case head :: rest => writer.partitionedBy(col(head), rest.map(col): _*)
    }
    partitioned.create()
  }
}
