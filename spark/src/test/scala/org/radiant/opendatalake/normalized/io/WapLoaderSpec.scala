package org.radiant.opendatalake.normalized.io

import bio.ferlab.datalake.commons.config.Format.ICEBERG
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{DatasetConf, TableConf}
import org.radiant.opendatalake.testutils.SparkSpec

case class WapRow(id: String, chromosome: String = "1", value: String = "first")

class WapLoaderSpec extends SparkSpec {

  import spark.implicits._

  /*
    A distinct table per test. WithSparkTestEnvironment wipes spark/tmp in beforeEach and afterEach while
    the SparkSession — and Iceberg's caching SparkCatalog — is a JVM singleton, so reusing a name across
    tests risks a cached handle to a table whose files have been deleted underneath it.

    No path/location is asserted anywhere: WapLoader creates tables through writeTo(...).create(), so the
    catalog assigns the default location and the Hadoop catalog's "no custom locations" constraint (the
    reason test.conf rewrites Iceberg paths) never comes into play.
  */
  private def dest(tableName: String, partitionBy: List[String] = Nil): DatasetConf =
    DatasetConf(
      id = s"wap_$tableName",
      storageid = "iceberg_storage",
      path = s"/$tableName",
      format = ICEBERG,
      loadtype = OverWrite,
      table = Some(TableConf("reference", tableName)),
      partitionby = partitionBy
    )

  private def fqn(ds: DatasetConf): String = s"${ds.table.get.database}.${ds.table.get.name}"

  private def refNames(ds: DatasetConf): Set[String] =
    spark.sql(s"SELECT name FROM ${fqn(ds)}.refs").collect().map(_.getString(0)).toSet

  private def onBranch(ds: DatasetConf, branch: String): Seq[WapRow] =
    spark.read.option("branch", branch).table(fqn(ds)).as[WapRow].collect().toSeq

  private def onMain(ds: DatasetConf): Seq[WapRow] = spark.table(fqn(ds)).as[WapRow].collect().toSeq

  "publish" should "create the table on first run and leave main empty" in {
    val ds = dest("wap_bootstrap")
    val rows = Seq(WapRow("1"), WapRow("2"))

    val published = WapLoader.publish(ds, rows.toDF(), "20260715")(spark)

    published.as[WapRow].collect() should contain theSameElementsAs rows
    onBranch(ds, "20260715") should contain theSameElementsAs rows
    withClue("main must stay empty — it is only a base to branch from: ") {
      onMain(ds) shouldBe empty
    }
  }

  it should "drop the transient audit branch" in {
    val ds = dest("wap_audit_dropped")
    WapLoader.publish(ds, Seq(WapRow("1")).toDF(), "20260715")(spark)

    val refs = refNames(ds)
    refs should contain("main")
    refs should contain("20260715")
    withClue(s"audit branch survived the run, refs were $refs: ") {
      refs should not contain WapLoader.auditBranch("20260715")
    }
    refs.filter(_.startsWith("audit")) shouldBe empty
  }

  it should "replace the branch, not append to it, when the same version is re-imported" in {
    val ds = dest("wap_reimport")
    val first = Seq(WapRow("1", value = "first"), WapRow("2", value = "first"))
    val second = Seq(WapRow("1", value = "second"))

    WapLoader.publish(ds, first.toDF(), "20260715")(spark)
    WapLoader.publish(ds, second.toDF(), "20260715")(spark)

    // SJRA-1546 §3.4: "Tables are idempotent. The same version re-runs will overwrite the data."
    onBranch(ds, "20260715") should contain theSameElementsAs second
    onMain(ds) shouldBe empty
  }

  it should "keep each dataset_version on its own independent branch" in {
    val ds = dest("wap_two_versions")
    val older = Seq(WapRow("1", value = "older"))
    val newer = Seq(WapRow("2", value = "newer"))

    WapLoader.publish(ds, older.toDF(), "20260708")(spark)
    WapLoader.publish(ds, newer.toDF(), "20260715")(spark)

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
    val ds = dest("wap_dotted_version")
    val rows = Seq(WapRow("1"))
    val version = "GCF_000001405.40"

    WapLoader.publish(ds, rows.toDF(), version)(spark)

    onBranch(ds, version) should contain theSameElementsAs rows
    refNames(ds) should contain(version)
    refNames(ds).filter(_.startsWith("audit")) shouldBe empty
  }

  it should "carry the destination's partitioning onto the created table" in {
    val ds = dest("wap_partitioned", partitionBy = List("chromosome"))
    val rows = Seq(WapRow("1", chromosome = "1"), WapRow("2", chromosome = "2"))

    WapLoader.publish(ds, rows.toDF(), "20260715")(spark)

    onBranch(ds, "20260715") should contain theSameElementsAs rows
    val partitionFields = spark.sql(s"SELECT * FROM ${fqn(ds)}.partitions").columns
    withClue(s"partition metadata columns were ${partitionFields.mkString(", ")}: ") {
      partitionFields.exists(_.contains("partition")) shouldBe true
    }
  }

  it should "reject a destination with no table" in {
    val tableless = dest("wap_tableless").copy(table = None)

    val ex = the[IllegalArgumentException] thrownBy WapLoader.publish(tableless, Seq(WapRow("1")).toDF(), "1")(spark)
    ex.getMessage should include("declares no table")
  }
}
