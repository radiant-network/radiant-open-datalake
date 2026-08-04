package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.DatasetConf

object ContractDestination {

  private def suffix(major: Int): String = s"_v$major"

  def tableName(tablePrefix: String, major: Int): String = tablePrefix + suffix(major)

  def forMajor(sourceDataset: DatasetConf, tablePrefix: String, major: Int): DatasetConf = {
    val anchor = sourceDataset.table.getOrElse(
      throw new IllegalArgumentException(s"${sourceDataset.id} declares no table")
    )

    val name = tableName(tablePrefix, major)

    sourceDataset.copy(
      id = sourceDataset.id + suffix(major),
      path = sourceDataset.path.take(sourceDataset.path.lastIndexOf('/') + 1) + name,
      table = Some(anchor.copy(name = name))
    )
  }
}
