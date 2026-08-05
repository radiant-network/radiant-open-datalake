package org.radiant.opendatalake.normalized.mondo

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import bio.ferlab.transform.DownloadTransformer
import org.apache.spark.sql.DataFrame
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.io.ByteArrayInputStream
import java.time.LocalDateTime
import scala.io.{BufferedSource, Codec}

case class MondoTerm(id: String,
                     name: String,
                     parents: Seq[String],
                     alt_ids: Seq[String])

case class Mondo_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String)
  extends ContractETLP(rc, sourceDatasetId = "normalized_mondo", tablePrefix, major = 1) {

  import spark.implicits._

  private val raw_mondo: DatasetConf = conf.getDataset("raw_mondo")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(raw_mondo.id -> RawInput.readVersioned(raw_mondo.id, version, rawStorage))

  private def oboSource(raw: DataFrame): BufferedSource = {
    val files: Array[Array[Byte]] = raw.select("content").as[Array[Byte]].collect()

    require(
      files.length == 1,
      s"Expected exactly one .obo for ${raw_mondo.id} at version '$version', found ${files.length}"
    )

    new BufferedSource(new ByteArrayInputStream(files.head))(Codec.UTF8)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val terms: Seq[MondoTerm] = DownloadTransformer.downloadOntologyData(oboSource(data(raw_mondo.id)))
      .filter(_.id.nonEmpty)
      .map(t => MondoTerm(t.id, t.name, t.parents.map(_.id), t.alternateIds))

    require(
      terms.nonEmpty,
      s"No Mondo term parsed from ${raw_mondo.id} at version '$version': the .obo is missing, empty, or not " +
        "readable as OBO (obo-parser reports a read failure as an empty result)"
    )

    spark.createDataset(terms).toDF()
  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
