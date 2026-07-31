package org.radiant.opendatalake.wap

import bio.ferlab.datalake.commons.config.{Configuration, DatasetConf, TableConf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.radiant.opendatalake.wap.iceberg.{IcebergDatabase, IcebergTable}
import org.slf4j.{Logger, LoggerFactory}

object WapLoader {

  private val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  def auditBranchOf(version: String): String = s"audit_$version"

  def publish(destination: DatasetConf, data: DataFrame, version: String)
             (implicit spark: SparkSession, conf: Configuration): DataFrame = {
    val table = IcebergTable(requireTable(destination))

    prepareCleanBase(table, data, destination.partitionby, destination.location)
    val auditSnapshotId = stageOnAuditBranch(table, data, version)
    publishVersionBranch(table, version, auditSnapshotId)
  }

  private def requireTable(destination: DatasetConf): TableConf =
    destination.table.getOrElse(
      throw new IllegalArgumentException(
        s"${destination.id} declares no table; a WAP destination must be an Iceberg table"
      )
    )

  private def prepareCleanBase(table: IcebergTable, data: DataFrame, partitionBy: List[String], location: String)
                              (implicit spark: SparkSession): Unit = {
    IcebergDatabase(table.database).createIfNotExists()

    if (table.exists()) {
      allowSchemaEvolution(table)
      ensureEmptyMain(table)
    } else {
      log.info(s"WAP ${table.fullName}: table absent, creating it empty on ${IcebergTable.MainBranch} at $location")
      table.createEmpty(data, partitionBy, location)
    }
  }

  private def allowSchemaEvolution(table: IcebergTable)(implicit spark: SparkSession): Unit =
    if (!table.schemaEvolutionEnabled()) {
      log.info(s"WAP ${table.fullName}: table predates schema evolution on write, enabling it")
      table.enableSchemaEvolution()
    }

  private def ensureEmptyMain(table: IcebergTable)(implicit spark: SparkSession): Unit =
    table.rowCount() match { // once per import, against ETLs that run for hours
      case 0L => ()
      case rows =>
        log.warn(s"WAP ${table.fullName}: ${IcebergTable.MainBranch} held $rows row(s); emptying it, it must stay a clean base")
        table.deleteAll()
    }

  private def stageOnAuditBranch(table: IcebergTable, data: DataFrame, version: String)
                                (implicit spark: SparkSession): Long = {
    val auditBranch = auditBranchOf(version)
    val mainSnapshotId = table.mainSnapshotId().getOrElse(
      throw new IllegalStateException(s"${table.fullName} has no '${IcebergTable.MainBranch}' ref after bootstrap")
    )

    log.info(s"WAP ${table.fullName}: staging '$version' on '$auditBranch' from ${IcebergTable.MainBranch}@$mainSnapshotId")

    table.createOrReplaceBranch(auditBranch, mainSnapshotId)
    table.overwriteBranch(auditBranch, data)

    table.snapshotIdOf(auditBranch).getOrElse(
      throw new IllegalStateException(s"audit branch '$auditBranch' vanished mid-run on ${table.fullName}")
    )
  }

  private def publishVersionBranch(table: IcebergTable, version: String, auditSnapshotId: Long)
                                  (implicit spark: SparkSession): DataFrame = {
    val auditBranch = auditBranchOf(version)

    log.info(s"WAP ${table.fullName}: publishing '$version' from '$auditBranch'@$auditSnapshotId, dropping it")

    table.createOrReplaceBranch(version, auditSnapshotId)
    table.dropBranch(auditBranch)

    table.readBranch(version)
  }
}
