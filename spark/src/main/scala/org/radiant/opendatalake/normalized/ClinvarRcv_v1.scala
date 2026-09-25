package org.radiant.opendatalake.normalized

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, functions}
import org.radiant.opendatalake.contracts.ContractETLP
import org.radiant.opendatalake.normalized.io.RawInput

import java.time.LocalDateTime

/**
  * ClinVar RCV summary: one row per aggregated RCV record, carrying its germline classification, the
  * conditions it was asserted against, and the SCV submissions it aggregates.
  *
  * Source: `ClinVarRCVRelease_{{VERSION}}.xml.gz` (monthly, first Thursday), the RCV half of the ClinVar
  * XML release. This is a different product from the ClinVar VCF that `Clinvar_v1` normalizes: the VCF is
  * keyed by locus and carries only the aggregated `CLNSIG`, whereas RCV is keyed by
  * (variant, condition) and is the only release that exposes the per-submitter classifications.
  *
  * Every value is derived with column expressions rather than a UDF, so the whole record is built in one
  * projection over the parsed XML and Catalyst can prune the struct fields that are never read.
  */
case class ClinvarRcv_v1(rc: RuntimeETLContext, version: String, rawStorage: String, tablePrefix: String, database: Option[String] = None, override val warehouse: Option[String] = None)
  extends ContractETLP(rc, sourceDatasetId = "normalized_clinvar_rcv", tablePrefix, major = 1, database) {

  private val clinvar_rcv_xml: DatasetConf = conf.getDataset("raw_clinvar_rcv")

  override def extract(lastRunValue: LocalDateTime = minValue,
                       currentRunValue: LocalDateTime = LocalDateTime.now()): Map[String, DataFrame] =
    Map(
      clinvar_rcv_xml.id ->
        RawInput.readVersionedWithSchema(clinvar_rcv_xml.id, version, rawStorage, ClinvarRcvXml.schema)
    )

  override def transformSingle(data: Map[String, DataFrame],
                               lastRunValue: LocalDateTime = minValue,
                               currentRunValue: LocalDateTime = LocalDateTime.now()): DataFrame = {

    val assertion = col("ReferenceClinVarAssertion")
    val germline = assertion("Classifications")("GermlineClassification")
    val description = germline("Description")
    val reviewStatus = trim(germline("ReviewStatus"))

    data(clinvar_rcv_xml.id)
      .select(
        variationId(assertion) as "clinvar_id",
        assertion("ClinVarAccession")(attr("Acc")) as "accession",
        assertion("ClinVarAccession")(attr("Version")) as "version",
        clinicalSignificance(description(ClinvarRcvXml.ValueTag)) as "clinical_significance",
        to_date(description(attr("DateLastEvaluated"))) as "date_last_evaluated",
        description(attr("SubmissionCount")) as "submission_count",
        reviewStatus as "review_status",
        reviewStatusStars(reviewStatus) as "review_status_stars",
        preferredTraitNames(assertion) as "traits",
        origins(assertion) as "origins",
        submissions(col("ClinVarAssertion")) as "submissions"
      )
      .withColumn(
        "clinical_significance_count",
        clinicalSignificanceCount(col("clinical_significance"), col("submissions"))
      )
  }

  private def attr(name: String): String = ClinvarRcvXml.AttributePrefix + name

  /**
    * The ClinVar variation id, from the `MeasureSet` of type `Variant`. Compound heterozygotes and
    * diplotypes carry a `GenotypeSet` instead, whose nested measure sets are searched in document order.
    * Records describing something other than a variant (a gene, an OMIM record) yield null.
    */
  private def variationId(assertion: Column): Column = {
    val direct = when(assertion("MeasureSet")(attr("Type")) === "Variant", assertion("MeasureSet")(attr("ID")))
    val nested = functions.filter(
      assertion("GenotypeSet")("MeasureSet"),
      measure => measure(attr("Type")) === "Variant"
    )
    // Guarded rather than a bare element_at: indexing past the end of an array raises under ANSI mode.
    coalesce(direct, when(size(nested) > 0, element_at(nested, 1)(attr("ID"))))
  }

  /**
    * ClinVar joins co-asserted classifications with `/` (`Pathogenic/Likely pathogenic`) and `;`, so the
    * aggregate description is published as the array of its parts. Null in, null out: a record with no
    * germline classification has no significance rather than an empty array.
    */
  private def clinicalSignificance(description: Column): Column =
    functions.transform(split(description, "[/;]"), part => trim(part))

  /**
    * The ClinVar review status expressed as its documented star rating.
    *
    * Ref: https://www.ncbi.nlm.nih.gov/clinvar/docs/review_status/ — every status the table does not name
    * (`no assertion criteria provided`, `no classification provided`, …) rates zero stars, as does a
    * record with no review status at all.
    */
  private def reviewStatusStars(reviewStatus: Column): Column =
    when(reviewStatus === "practice guideline", lit(4))
      .when(reviewStatus === "reviewed by expert panel", lit(3))
      .when(reviewStatus === "criteria provided, multiple submitters, no conflicts", lit(2))
      .when(reviewStatus === "criteria provided, single submitter", lit(1))
      .when(reviewStatus === "criteria provided, conflicting classifications", lit(1))
      .otherwise(lit(0))

  /** The preferred name of every trait the record was asserted against, in document order. */
  private def preferredTraitNames(assertion: Column): Column = {
    val perTrait = functions.transform(
      assertion("TraitSet")("Trait"),
      traitElement => functions.transform(
        functions.filter(traitElement("Name"), name => name("ElementValue")(attr("Type")) === "Preferred"),
        name => name("ElementValue")(ClinvarRcvXml.ValueTag)
      )
    )
    nonEmptyStrings(flatten(perTrait))
  }

  /** The distinct allele origins observed across the record's `ObservedIn` samples, sorted. */
  private def origins(assertion: Column): Column = {
    val observed = functions.transform(
      assertion("ObservedIn"),
      observation => trim(observation("Sample")("Origin"))
    )
    array_sort(array_distinct(nonEmptyStrings(observed)))
  }

  /** Drop nulls and blanks, and read an absent array as an empty one rather than null. */
  private def nonEmptyStrings(values: Column): Column =
    coalesce(
      functions.filter(values, value => value.isNotNull && length(value) > 0),
      typedLit(Seq.empty[String])
    )

  /** The SCV submissions aggregated by the record, one struct each. */
  private def submissions(assertions: Column): Column =
    functions.transform(assertions, submission => {
      val reviewStatus = trim(submission("Classification")("ReviewStatus"))
      struct(
        submission("ClinVarSubmissionID")(attr("submitter")) as "submitter",
        submission("ClinVarAccession")(attr("Acc")) as "scv",
        submission("ClinVarAccession")(attr("Version")) as "version",
        reviewStatus as "review_status",
        reviewStatusStars(reviewStatus) as "review_status_stars",
        trim(submission("Classification")("GermlineClassification")) as "clinical_significance",
        to_date(submission("Classification")(attr("DateLastEvaluated"))) as "date_last_evaluated"
      )
    })

  /**
    * How many submissions back each part of the aggregate classification, e.g.
    * `{"Pathogenic": 3, "Likely pathogenic": 1}` for a `Pathogenic/Likely pathogenic` record.
    *
    * Only submitted classifications that appear in the aggregate are counted, so a submission whose
    * classification ClinVar did not carry into the aggregate (`Uncertain significance` under a
    * `Pathogenic` record) is absent from the map rather than counted at zero.
    */
  private def clinicalSignificanceCount(significance: Column, submissions: Column): Column = {
    val submitted = functions.transform(submissions, submission => submission("clinical_significance"))
    val counted = functions.filter(
      array_distinct(submitted),
      value => value.isNotNull && array_contains(significance, value)
    )
    map_from_entries(
      functions.transform(
        counted,
        value => struct(
          value as "key",
          size(functions.filter(submitted, submittedValue => submittedValue === value)) as "value"
        )
      )
    )
  }
}
