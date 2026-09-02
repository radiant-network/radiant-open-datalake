package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.{DatasetConf, TableConf}

object ContractDestination {

  private def suffix(major: Int): String = s"_v$major"

  def tableName(tablePrefix: String, major: Int): String = tablePrefix + suffix(major)

  def forMajor(sourceDataset: DatasetConf, tablePrefix: String, major: Int, database: Option[String] = None): DatasetConf = {
    val name = tableName(tablePrefix, major)

    val anchor: TableConf = sourceDataset.table.getOrElse(
      throw new IllegalArgumentException(s"${sourceDataset.id} declares no table")
    )

    sourceDataset.copy(
      id = sourceDataset.id + suffix(major),
      path = sourceDataset.path.take(sourceDataset.path.lastIndexOf('/') + 1) + name,
      table = Some(anchor.copy(name = name, database = database.getOrElse(anchor.database)))
    )
  }
}
