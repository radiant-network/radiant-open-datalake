package org.radiant.opendatalake.normalized

import org.apache.spark.sql.types._

/**
  * The subtree of a ClinVar RCV release `<ClinVarSet>` element that [[ClinvarRcv_v1]] reads.
  *
  * Declared rather than inferred. The release is one ~6 GB gzip member, so letting the reader infer a
  * schema would cost a second full parse of it, and the inferred shape would be the union of every
  * element ClinVar has ever emitted — thousands of columns, and a shape that changes under the job
  * whenever upstream adds an element. Reading with this schema also makes spark-xml skip the children of
  * every element that is absent from it (`StaxXmlParser` has nowhere to put them), so the ObservedData
  * free-text blocks — by far the bulk of the file — are never materialised.
  *
  * Names are the XML names verbatim, attributes carry the `_` prefix and element text the `_VALUE` tag
  * (spark-xml's `attributePrefix` / `valueTag` defaults, pinned in the dataset's `readoptions`). Field
  * order is irrelevant; spark-xml matches by name.
  *
  * Cardinalities follow `ClinVar_RCV.xsd` (`ftp.ncbi.nlm.nih.gov/pub/clinvar/xsd_public/RCV/`): a
  * `ClinVarSet` carries at most one `ReferenceClinVarAssertion` (the aggregate RCV record) and one or
  * more `ClinVarAssertion` (the SCV submissions underlying it).
  */
object ClinvarRcvXml {

  /** `rowTag`: one output row per aggregated RCV record plus its submissions. */
  val RowTag: String = "ClinVarSet"

  val AttributePrefix: String = "_"
  val ValueTag: String = "_VALUE"

  /** `<ClinVarAccession Acc="RCV000000010" Version="3"/>` — also the SCV accession on a submission. */
  private val accession = StructType(Seq(
    StructField(AttributePrefix + "Acc", StringType),
    StructField(AttributePrefix + "Version", IntegerType)
  ))

  /** `<Description DateLastEvaluated="2000-04-01" SubmissionCount="1">Pathogenic</Description>` */
  private val description = StructType(Seq(
    StructField(AttributePrefix + "DateLastEvaluated", StringType),
    StructField(AttributePrefix + "SubmissionCount", IntegerType),
    StructField(ValueTag, StringType)
  ))

  /**
    * `Classifications/GermlineClassification`, the aggregate classification. Its siblings
    * `SomaticClinicalImpact`, `OncogenicityClassification` and `NoClassification` are deliberately left
    * out: MAJOR 1 publishes the germline classification only.
    */
  private val germlineClassification = StructType(Seq(
    StructField("ReviewStatus", StringType),
    StructField("Description", description)
  ))

  /** `<ElementValue Type="Preferred">…</ElementValue>`, the single child of a `SetElementSetType`. */
  private val elementValue = StructType(Seq(
    StructField(AttributePrefix + "Type", StringType),
    StructField(ValueTag, StringType)
  ))

  private val setElement = StructType(Seq(StructField("ElementValue", elementValue)))

  /** A `TraitSet` holds one or more `Trait`, each with zero or more `Name`. */
  private val traitSet = StructType(Seq(
    StructField("Trait", ArrayType(StructType(Seq(StructField("Name", ArrayType(setElement))))))
  ))

  /**
    * `<MeasureSet Type="Variant" ID="18397">`: `ID` is the ClinVar variation id, and the two attributes
    * are the only values this contract reads out of it.
    *
    * `Name` is declared regardless, and must stay declared. spark-xml treats a struct whose every field
    * is an attribute or the value tag as an element that cannot have child elements, and returns from it
    * without consuming any: `MeasureSet`'s real children (`Measure`, `Name`, …) then surface in the
    * enclosing element's parse loop, where the first of them that the enclosing schema does not name
    * ends that element early. Concretely, dropping this field silently empties `TraitSet` — the sibling
    * declared after `MeasureSet` — and reduces a `GenotypeSet` to its first measure set.
    * Ref: `StaxXmlParser.convertField`, the `case (c: Characters, st: StructType)` branch.
    */
  private val measureSet = StructType(Seq(
    StructField(AttributePrefix + "ID", StringType),
    StructField(AttributePrefix + "Type", StringType),
    StructField("Name", ArrayType(setElement))
  ))

  /**
    * The assertion carries either a `MeasureSet` or a `GenotypeSet` (an XSD `xs:choice`); a `GenotypeSet`
    * wraps the `MeasureSet`s of a compound heterozygote or diplotype.
    */
  private val genotypeSet = StructType(Seq(StructField("MeasureSet", ArrayType(measureSet))))

  private val observedIn = StructType(Seq(
    StructField("Sample", StructType(Seq(StructField("Origin", StringType))))
  ))

  private val referenceClinVarAssertion = StructType(Seq(
    StructField("ClinVarAccession", accession),
    StructField("Classifications", StructType(Seq(StructField("GermlineClassification", germlineClassification)))),
    StructField("ObservedIn", ArrayType(observedIn)),
    StructField("MeasureSet", measureSet),
    StructField("GenotypeSet", genotypeSet),
    StructField("TraitSet", traitSet)
  ))

  /**
    * A submitted record (SCV). Its classification element is `Classification` (singular, with the
    * classification as plain text), not the aggregate's `Classifications` wrapper.
    */
  private val clinVarAssertion = StructType(Seq(
    StructField("ClinVarSubmissionID", StructType(Seq(StructField(AttributePrefix + "submitter", StringType)))),
    StructField("ClinVarAccession", accession),
    StructField("Classification", StructType(Seq(
      StructField(AttributePrefix + "DateLastEvaluated", StringType),
      StructField("ReviewStatus", StringType),
      StructField("GermlineClassification", StringType)
    )))
  ))

  val schema: StructType = StructType(Seq(
    StructField("ReferenceClinVarAssertion", referenceClinVarAssertion),
    StructField("ClinVarAssertion", ArrayType(clinVarAssertion))
  ))
}
