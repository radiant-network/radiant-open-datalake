package org.radiant.opendatalake.wap

import bio.ferlab.datalake.commons.config.Format.ICEBERG
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{DatasetConf, TableConf}
import org.apache.spark.sql.functions.col
import org.radiant.opendatalake.testutils.SparkSpec

case class WapRow(id: String, chromosome: String = "1", value: String = "first")

/** `WapRow` plus one column, standing in for the added column of a MINOR contract bump. */
case class WiderWapRow(id: String,
                       chromosome: String = "1",
                       value: String = "first",
                       clinical_significance: String = "benign")

class WapLoaderSpec extends SparkSpec {

  import spark.implicits._

  /*
    A distinct table per test. WithSparkTestEnvironment wipes spark/tmp in beforeEach and afterEach while
    the SparkSession — and Iceberg's caching SparkCatalog — is a JVM singleton, so reusing a name across
    tests risks a cached handle to a table whose files have been deleted underneath it.

    `path` is `/<table-name>` for the same reason test.conf rewrites every Iceberg dataset that way: WapLoader
    creates the table *at* the declared location, and the Hadoop catalog accepts one only when it matches
    `<warehouse>/<namespace>/<table>` exactly.
  */
  private def icebergDataset(tableName: String, partitionBy: List[String] = Nil): DatasetConf =
    DatasetConf(
      id = s"wap_$tableName",
      storageid = "iceberg_storage",
      path = s"/$tableName",
      format = ICEBERG,
      loadtype = OverWrite,
      table = Some(TableConf("reference", tableName)),
      partitionby = partitionBy
    )

  private def tableNameOf(ds: DatasetConf): String = ds.table.get.fullName

  private def refNames(ds: DatasetConf): Set[String] =
    spark.sql(s"SELECT name FROM ${tableNameOf(ds)}.refs").collect().map(_.getString(0)).toSet

  private def onBranch(ds: DatasetConf, branch: String): Seq[WapRow] =
    spark.read.option("branch", branch).table(tableNameOf(ds)).as[WapRow].collect().toSeq

  private def onMain(ds: DatasetConf): Seq[WapRow] = spark.table(tableNameOf(ds)).as[WapRow].collect().toSeq

  private def locationOf(ds: DatasetConf): String =
    spark
      .sql(s"DESCRIBE TABLE EXTENDED ${tableNameOf(ds)}")
      .where(col("col_name") === "Location")
      .head()
      .getAs[String]("data_type")

  "publish" should "create the table on first run and leave main empty" in {
    val ds = icebergDataset("wap_bootstrap")
    val rows = Seq(WapRow("1"), WapRow("2"))

    val published = WapLoader.publish(ds, rows.toDF(), "20260715")

    published.as[WapRow].collect() should contain theSameElementsAs rows
    onBranch(ds, "20260715") should contain theSameElementsAs rows
    withClue("main must stay empty — it is only a base to branch from: ") {
      onMain(ds) shouldBe empty
    }
  }

  it should "drop the transient audit branch" in {
    val ds = icebergDataset("wap_audit_dropped")
    WapLoader.publish(ds, Seq(WapRow("1")).toDF(), "20260715")

    val refs = refNames(ds)
    refs should contain("main")
    refs should contain("20260715")
    withClue(s"audit branch survived the run, refs were $refs: ") {
      refs should not contain WapLoader.auditBranchOf("20260715")
    }
    refs.filter(_.startsWith("audit")) shouldBe empty
  }

  it should "replace the branch, not append to it, when the same version is re-imported" in {
    val ds = icebergDataset("wap_reimport")
    val first = Seq(WapRow("1", value = "first"), WapRow("2", value = "first"))
    val second = Seq(WapRow("1", value = "second"))

    WapLoader.publish(ds, first.toDF(), "20260715")
    WapLoader.publish(ds, second.toDF(), "20260715")

    // SJRA-1546 §3.4: "Tables are idempotent. The same version re-runs will overwrite the data."
    onBranch(ds, "20260715") should contain theSameElementsAs second
    onMain(ds) shouldBe empty
  }

  it should "keep each dataset_version on its own independent branch" in {
    val ds = icebergDataset("wap_two_versions")
    val older = Seq(WapRow("1", value = "older"))
    val newer = Seq(WapRow("2", value = "newer"))

    WapLoader.publish(ds, older.toDF(), "20260708")
    WapLoader.publish(ds, newer.toDF(), "20260715")

    onBranch(ds, "20260708") should contain theSameElementsAs older
    onBranch(ds, "20260715") should contain theSameElementsAs newer
    refNames(ds) should contain allOf ("main", "20260708", "20260715")
    onMain(ds) shouldBe empty
  }

  /*
    dbsnp's dataset_version is a RefSeq accession, "GCF_000001405.40". The dot makes it an invalid bare
    SQL identifier, so every ref name in WapLoader's DDL is backtick-quoted; this is the regression test
    for that. clinvar's all-digit versions are the same problem in a different disguise and are covered by
    the tests above.
  */
  it should "handle a dataset_version containing a dot" in {
    val ds = icebergDataset("wap_dotted_version")
    val rows = Seq(WapRow("1"))
    val version = "GCF_000001405.40"

    WapLoader.publish(ds, rows.toDF(), version)

    onBranch(ds, version) should contain theSameElementsAs rows
    refNames(ds) should contain(version)
    refNames(ds).filter(_.startsWith("audit")) shouldBe empty
  }

  it should "carry the destination's partitioning onto the created table" in {
    val ds = icebergDataset("wap_partitioned", partitionBy = List("chromosome"))
    val rows = Seq(WapRow("1", chromosome = "1"), WapRow("2", chromosome = "2"))

    WapLoader.publish(ds, rows.toDF(), "20260715")

    onBranch(ds, "20260715") should contain theSameElementsAs rows
    val partitionFields = spark.sql(s"SELECT * FROM ${tableNameOf(ds)}.partitions").columns
    withClue(s"partition metadata columns were ${partitionFields.mkString(", ")}: ") {
      partitionFields.exists(_.contains("partition")) shouldBe true
    }
  }

  /*
    Regression: a table written by the pre-WAP overwrite path has its rows on main. Because audit is cut
    from main, an append there inherited those rows and published 2x on the version branch while main kept
    the originals. main must be emptied first, and the audit write must not depend on having done so.
  */
  it should "not double the rows when the table already has legacy data on main" in {
    val ds = icebergDataset("wap_legacy_main")
    val legacy = Seq(WapRow("1"), WapRow("2"), WapRow("3"))

    // Simulate the old ETL: SaveMode.Overwrite straight onto main.
    legacy.toDF().writeTo(tableNameOf(ds)).create()
    onMain(ds) should have size 3

    WapLoader.publish(ds, legacy.toDF(), "20260715")

    onBranch(ds, "20260715") should contain theSameElementsAs legacy
    withClue("legacy main rows were inherited by the audit branch and published twice: ") {
      onBranch(ds, "20260715") should have size 3
    }
    onMain(ds) shouldBe empty
  }

  it should "leave already-published version branches intact when it empties main" in {
    val ds = icebergDataset("wap_empty_main_keeps_branches")
    val older = Seq(WapRow("1", value = "older"))

    WapLoader.publish(ds, older.toDF(), "20260708")

    // Something puts rows back on main after that first publish.
    Seq(WapRow("9", value = "stray")).toDF().writeTo(tableNameOf(ds)).append()
    onMain(ds) should have size 1

    WapLoader.publish(ds, Seq(WapRow("2", value = "newer")).toDF(), "20260715")

    onMain(ds) shouldBe empty
    withClue("emptying main clobbered a previously published version branch: ") {
      onBranch(ds, "20260708") should contain theSameElementsAs older
    }
  }

  /*
    SJRA-1546 bumps a contract's MINOR in place when a source adds columns, so the audit write has to widen
    the table rather than fail. That needs two settings, and neither works alone: the accept-any-schema table
    property, or Spark's analyzer rejects the extra column before Iceberg sees the write; and the merge-schema
    write option, or Iceberg validates against the old schema instead of unioning with it.
  */
  it should "widen the table when a later version adds a column" in {
    val ds = icebergDataset("wap_added_column")
    val wider = Seq(WiderWapRow("2", value = "newer", clinical_significance = "pathogenic"))

    WapLoader.publish(ds, Seq(WapRow("1", value = "older")).toDF(), "20260708")
    WapLoader.publish(ds, wider.toDF(), "20260715")

    spark.read.option("branch", "20260715").table(tableNameOf(ds)).as[WiderWapRow].collect() should
      contain theSameElementsAs wider

    withClue("widening the schema must not disturb an already-published branch: ") {
      val older = spark.read.option("branch", "20260708").table(tableNameOf(ds)).collect()
      older.map(_.getAs[String]("id")).toSeq shouldBe Seq("1")
      // The column is table-level metadata, so it appears on the older branch — with no value behind it.
      older.head.getAs[String]("clinical_significance") shouldBe null
    }
  }

  /*
    accept-any-schema makes Spark skip TableOutputResolver, which is what normally reorders a by-name write
    into the table's column order — so the danger is that a version emitting the same columns in a different
    order writes each value into its neighbour's column. It does not: Iceberg resolves names to field ids
    first (the rejection below reports value->3, chromosome->2, id->1, i.e. correctly matched), then refuses
    on ordering alone, because check-ordering defaults to true. Reordering is therefore a loud failure and
    never silent corruption — a MINOR bump may add columns, but must not reorder them.
  */
  it should "refuse a reordered write rather than shift values into neighbouring columns" in {
    val ds = icebergDataset("wap_reordered_columns")

    WapLoader.publish(ds, Seq(WapRow(id = "1", chromosome = "X", value = "older")).toDF(), "20260708")

    val reordered = Seq(("newer", "Y", "2")).toDF("value", "chromosome", "id")
    val ex = the[IllegalArgumentException] thrownBy WapLoader.publish(ds, reordered, "20260715")

    ex.getMessage should include("Cannot write incompatible dataset to table with schema")
    withClue("the failed write must not have published anything: ") {
      refNames(ds) should not contain "20260715"
    }
  }

  it should "widen a table created before the accept-any-schema property was set" in {
    val ds = icebergDataset("wap_added_column_legacy")
    val wider = Seq(WiderWapRow("2", clinical_significance = "pathogenic"))

    // Pre-WAP creation: rows on main and none of the properties WapLoader now sets at create time.
    Seq(WapRow("1")).toDF().writeTo(tableNameOf(ds)).create()

    WapLoader.publish(ds, wider.toDF(), "20260715")

    spark.read.option("branch", "20260715").table(tableNameOf(ds)).as[WiderWapRow].collect() should
      contain theSameElementsAs wider
    onMain(ds) shouldBe empty
  }

  /*
    The bootstrapped table must land where EtlConfiguration says the dataset lives, not wherever the catalog
    would default to — Glue defaults to <warehouse>/<db>.db/<table>, which is not the declared path. The
    FerLab load path this replaced passed it as `.option("path", location).saveAsTable(...)`.
  */
  it should "create the table at the dataset's declared location" in {
    val ds = icebergDataset("wap_location")

    WapLoader.publish(ds, Seq(WapRow("1")).toDF(), "20260715")

    locationOf(ds) shouldBe s"file:${ds.location}"
  }

  /*
    Guards the plumbing rather than the value: under the Hadoop catalog a declared location that matches the
    catalog default is indistinguishable from passing none at all, so the assertion above would still pass if
    the location silently stopped being forwarded. A deliberately mismatched location must be rejected — only
    reachable if Iceberg is actually reading it.
  */
  it should "pass the declared location to Iceberg rather than let the catalog choose" in {
    val elsewhere = icebergDataset("wap_location_elsewhere").copy(path = "/somewhere/else")

    val ex = the[IllegalArgumentException] thrownBy
      WapLoader.publish(elsewhere, Seq(WapRow("1")).toDF(), "20260715")

    ex.getMessage should include("Cannot set a custom location")
    ex.getMessage should include(elsewhere.location)
  }

  it should "reject a destination with no table" in {
    val tableless = icebergDataset("wap_tableless").copy(table = None)

    val ex = the[IllegalArgumentException] thrownBy WapLoader.publish(tableless, Seq(WapRow("1")).toDF(), "1")
    ex.getMessage should include("declares no table")
  }
}
