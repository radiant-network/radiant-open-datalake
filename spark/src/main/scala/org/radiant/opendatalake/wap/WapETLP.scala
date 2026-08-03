package org.radiant.opendatalake.wap

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.etl.v4.SimpleETLP
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

abstract class WapETLP(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  def version: String

  override def loadSingle(data: DataFrame, lastRunValue: LocalDateTime, currentRunValue: LocalDateTime): DataFrame = {
    val repartitionFunc = mainDestination.repartition.getOrElse(defaultRepartition)
    WapLoader.publish(mainDestination, repartitionFunc(data), version)
  }
}
