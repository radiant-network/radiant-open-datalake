package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.RuntimeETLContext
import bio.ferlab.datalake.spark3.etl.v4.SimpleETLP
import org.apache.spark.sql.DataFrame
import org.radiant.opendatalake.normalized.io.WapLoader

import java.time.LocalDateTime

/*
  Base for a normalizer whose destination is versioned by Iceberg branch (SJRA-1546 §2.1).

  Extending this instead of SimpleETLP *is* the guarantee that the job cannot publish to `main`:
  `SingleETL` makes `transform` and `load` final, so `loadSingle` is the only seam below them, and
  overriding it here routes every write through WapLoader. A subclass would have to re-override
  `loadSingle` to get back to the framework's table-replacing load path.

  An abstract class rather than a trait: `SimpleETLP` takes a constructor argument, which a Scala 2
  trait cannot pass.
*/
abstract class WapETLP(rc: RuntimeETLContext) extends SimpleETLP(rc) {

  /** The dataset_version being imported. Names the published branch verbatim. */
  def version: String

  /*
    No default arguments — overriding a method that declares them is a compile error, and callers still
    resolve the defaults from SingleETL. Bypassing loadDataset means re-applying the repartition it would
    have done; the CREATE DATABASE it also did lives in WapLoader.publish.
  */
  override def loadSingle(data: DataFrame,
                          lastRunValue: LocalDateTime,
                          currentRunValue: LocalDateTime): DataFrame = {
    val repartition = mainDestination.repartition.getOrElse(defaultRepartition)
    WapLoader.publish(mainDestination, repartition(data), version)(spark)
  }
}
