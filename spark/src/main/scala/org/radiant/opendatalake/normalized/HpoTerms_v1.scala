package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{Coalesce, DatasetConf, RuntimeETLContext}
import bio.ferlab.transform.DownloadTransformer
import org.apache.spark.sql.DataFrame
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.io.ByteArrayInputStream
import java.time.LocalDateTime
import scala.io.{BufferedSource, Codec}

case class HpoTerm(id: String,
                   name: String,
                   parents: Seq[String],
                   alt_ids: Seq[String])

case class HpoTerms_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, override val warehouse: Option[String] = None)
  extends ContractETLP(rc, sourceDatasetId = "normalized_hpo_terms", tablePrefix, major = 1, database) {

  import spark.implicits._

  private val raw_hpo_terms: DatasetConf = conf.getDataset("raw_hpo_terms")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(raw_hpo_terms.id -> RawInput.readVersioned(raw_hpo_terms.id, version, rawStorage))

  private def oboSource(raw: DataFrame): BufferedSource = {
    val files: Array[Array[Byte]] = raw.select("content").as[Array[Byte]].collect()

    require(
      files.length == 1,
      s"Expected exactly one .obo for ${raw_hpo_terms.id} at version '$version', found ${files.length}"
    )

    new BufferedSource(new ByteArrayInputStream(files.head))(Codec.UTF8)
  }

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {
    val terms: Seq[HpoTerm] = DownloadTransformer.downloadOntologyData(oboSource(data(raw_hpo_terms.id)))
      .filter(_.id.nonEmpty)
      .map(t => HpoTerm(t.id, t.name, t.parents.map(_.id), t.alternateIds))

    require(
      terms.nonEmpty,
      s"No HPO term parsed from ${raw_hpo_terms.id} at version '$version': the .obo is missing, empty, or not " +
        "readable as OBO (obo-parser reports a read failure as an empty result)"
    )

    spark.createDataset(terms).toDF()
  }

  override val defaultRepartition: DataFrame => DataFrame = Coalesce()
}
