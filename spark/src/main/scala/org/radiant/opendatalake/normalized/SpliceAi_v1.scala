package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RepartitionByRange, RuntimeETLContext}
import org.apache.spark.sql.DataFrame
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.enriched.{SpliceAi => EnrichedSpliceAi}
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

/**
 * SpliceAI precomputed delta scores, contract MAJOR 1 (published to `spliceai_v1`).
 *
 * Illumina distributes the scores as two VCFs (SNV and indel) with identical schemas; the raw dataset
 * globs both (`spliceai_scores.raw.*.hg38.vcf.gz`) so a single read unions them into one table. Each row
 * is one variant-gene pair with the four acceptor/donor delta scores (`ds_*`), their positions (`dp_*`)
 * and a `max_score` summarising the strongest event. Composes the shared pure transforms
 * [[SpliceAi.normalize]] and [[EnrichedSpliceAi.addMaxScore]].
 *
 * Ref: Jaganathan et al., Cell 2019 — "Predicting Splicing from Primary Sequence with Deep Learning"
 *      https://doi.org/10.1016/j.cell.2018.12.015 ; scores hosted on BaseSpace (github.com/Illumina/SpliceAI).
 */
case class SpliceAi_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String)
  extends ContractETLP(rc, sourceDatasetId = "normalized_spliceai", tablePrefix, major = 1) {

  val raw_spliceai: DatasetConf = conf.getDataset("raw_spliceai")

  override def extract(lastRunValue: LocalDateTime,
                       currentRunValue: LocalDateTime): Map[String, DataFrame] =
    Map(raw_spliceai.id -> RawInput.readVersioned(raw_spliceai.id, version, rawStorage))

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime,
                               currentRunValue: LocalDateTime): DataFrame =
    EnrichedSpliceAi.addMaxScore(SpliceAi.normalize(data(raw_spliceai.id)))

  override def defaultRepartition: DataFrame => DataFrame =
    RepartitionByRange(columnNames = Seq("chromosome", "start"), n = Some(1000))
}
