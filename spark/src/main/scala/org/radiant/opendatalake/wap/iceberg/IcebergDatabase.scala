package org.radiant.opendatalake.wap.iceberg

import org.apache.spark.sql.SparkSession


case class IcebergDatabase(name: String) {

  def createIfNotExists()(implicit spark: SparkSession): Unit =
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $name")

  def table(tableName: String): IcebergTable = IcebergTable(name, tableName)
}
