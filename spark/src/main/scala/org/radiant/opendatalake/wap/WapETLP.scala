package org.radiant.opendatalake.wap

import bio.ferlab.datalake.commons.config.{Configuration, DatalakeConf, RuntimeETLContext, SimpleConfiguration}
import bio.ferlab.datalake.spark3.etl.v4.SimpleETLP
import org.apache.spark.sql.DataFrame

import java.time.LocalDateTime

abstract class WapETLP(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  def version: String

  /** Optional override of the Iceberg storage root (warehouse) the table is created at. When set, it
    * replaces the destination storage's root at write time — the destination `location` WapLoader passes
    * to `createEmpty` becomes `<warehouse>/<dataset path>`. Absent, the config's baked root is used. */
  def warehouse: Option[String] = None

  override def loadSingle(data: DataFrame, lastRunValue: LocalDateTime, currentRunValue: LocalDateTime): DataFrame = {
    val repartitionFunc = mainDestination.repartition.getOrElse(defaultRepartition)
    val effectiveConf = warehouse.fold(conf)(root => WapETLP.withStorageRoot(conf, mainDestination.storageid, root))
    WapLoader.publish(mainDestination, repartitionFunc(data), version)(spark, effectiveConf)
  }
}

object WapETLP {

  /** Rebuild the configuration with `storageId`'s root replaced. Mirrors `RawInput.readVersioned` for the
    * write side: only the destination `location` resolution depends on the storage root, so nothing else
    * in the config needs to change. */
  private[wap] def withStorageRoot(conf: Configuration, storageId: String, root: String): SimpleConfiguration =
    SimpleConfiguration(DatalakeConf(
      storages = conf.storages.map(storage => if (storage.id == storageId) storage.copy(path = root) else storage),
      sources = conf.sources,
      args = conf.args,
      sparkconf = conf.sparkconf
    ))
}
