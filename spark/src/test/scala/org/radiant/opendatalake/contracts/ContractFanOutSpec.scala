package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.Format.ICEBERG
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{DatasetConf, RunStep, RuntimeETLContext, TableConf}
import bio.ferlab.datalake.testutils.TestETLContext
import org.apache.spark.sql.DataFrame
import org.radiant.opendatalake.config.contracts.Contracts
import org.radiant.opendatalake.testutils.{CreateDatabasesBeforeAll, SparkSpec}
import org.radiant.opendatalake.wap.WapETLP

import java.time.LocalDateTime
import scala.util.Try

case class DemoRow(id: String, major: Int)

/*
  SJRA-1546 §3.4: a new MAJOR is a new Iceberg table, and the previous MAJOR keeps ingesting alongside it —
  from one dataset family, since each MAJOR derives its own destination from it. This drives the real entry
  point, ContractRunner.run, over a source declaring two MAJORs, through both of its injectable seams
  (`contracts` and `factories`), so exercising the fan-out needs no row in contracts.yml and no dataset in
  EtlConfiguration.
*/
class ContractFanOutSpec extends SparkSpec with CreateDatabasesBeforeAll {

  import spark.implicits._

  // WapLoader does not create it: the database is infrastructure, provisioned before the ETL ever runs.
  override val dbToCreate: List[String] = List("reference")

  private val datasetVersion = "20260715"

  /*
    A distinct source — and therefore a distinct family and pair of tables — per test. WithSparkTestEnvironment
    wipes spark/tmp between tests while the SparkSession, and Iceberg's caching SparkCatalog, is a JVM
    singleton, so reusing a table name risks a cached handle to a table whose files are gone.

    `path` is the table name for the same reason test.conf rewrites every Iceberg dataset that way: WapLoader
    creates the table *at* the declared location, and the Hadoop catalog accepts one only when it matches
    `<warehouse>/<namespace>/<table>` exactly. ContractDestination appends the MAJOR suffix to both.
  */
  private def familyOf(source: String): DatasetConf =
    DatasetConf(
      id = s"normalized_$source",
      storageid = "iceberg_storage",
      path = s"/$source",
      format = ICEBERG,
      loadtype = OverWrite,
      table = Some(TableConf("reference", source))
    )

  /*
    Deliberately *not* the source name, and not the family's table name either: table_prefix is what names the
    published table, so the whole fan-out is exercised with a name only contracts.yml knows.
  */
  private def prefixOf(source: String): String = s"${source}_open"

  private def tablesOf(source: String): List[String] =
    List(1, 2).map(ContractDestination.tableName(prefixOf(source), _))

  /** Stands in for a normalizer: whatever schema and rows a MAJOR happens to publish. */
  private case class FixedRowsNormalizer(rc: RuntimeETLContext,
                                         version: String,
                                         destination: DatasetConf,
                                         rows: DataFrame) extends WapETLP(rc) {

    override val mainDestination: DatasetConf = destination

    override def extract(lastRunValue: LocalDateTime, currentRunValue: LocalDateTime): Map[String, DataFrame] =
      Map(destination.id -> rows)

    override def transformSingle(data: Map[String, DataFrame],
                                 lastRunValue: LocalDateTime,
                                 currentRunValue: LocalDateTime): DataFrame = data(destination.id)
  }

  private def twoMajorsOf(source: String): Contracts = Contracts.parse(
    s"""sources:
       |  $source:
       |    table_prefix: "${prefixOf(source)}"
       |    contracts:
       |      - lineage: "1.4"
       |        release_notes: "v1.md"
       |      - lineage: "2.0"
       |        release_notes: "v2.md"
       |""".stripMargin
  )

  private def rowsOf(major: Int): Seq[DemoRow] = Seq(DemoRow(s"row-$major", major))

  /*
    The behaviour under test: every MAJOR starts from the same family and narrows it through
    ContractDestination using the prefix the runner resolved, exactly as ContractETLP does for a real
    normalizer. Taking the prefix from `args` rather than closing over it is the point — it proves the name
    reaches the normalizer from contracts.yml.
  */
  private def derivingFactories(source: String): ContractRunner.FactoryLookup = (_, contract) =>
    Some { args =>
      val destination = ContractDestination.forMajor(familyOf(source), args.tablePrefix, contract.major)
      FixedRowsNormalizer(args.rc, args.version, destination, rowsOf(contract.major).toDF())
    }

  // `publish` is a no-op for these datasets; the three steps that matter are enough.
  private def runFanOut(source: String, factories: ContractRunner.FactoryLookup): Unit =
    ContractRunner.run(
      source,
      TestETLContext(Seq(RunStep.extract, RunStep.transform, RunStep.load)),
      version = datasetVersion,
      rawStorage = "",
      contracts = twoMajorsOf(source),
      factories = factories
    )

  private def runFanOut(source: String): Unit = runFanOut(source, derivingFactories(source))

  private def onVersionBranch(table: String): Seq[DemoRow] =
    spark.read.option("branch", datasetVersion).table(s"reference.$table").as[DemoRow].collect().toSeq

  private def refNames(table: String): Set[String] =
    spark.sql(s"SELECT name FROM reference.$table.refs").collect().map(_.getString(0)).toSet

  /*
    The real contracts.yml and the real registry, built but not run — nothing is extracted, transformed or
    written. `build` is the only place the whole chain meets: it resolves each normalizer's dataset family
    (which is named in Scala, so no config-only check can reach it) and runs destinationMismatchReason, the check
    that catches a MAJOR N+1 class copied from MAJOR N and left declaring `major = N`. `plan` stops short of
    constructing jobs and so cannot see a destination at all — that gap let exactly that bug reach EMR.

    test.conf is enough: both confs are generated from the same `sources` list, so a family that is missing
    or renamed here is missing there too, stale regeneration included.
  */
  "build" should "resolve every contract declared in contracts.yml to a correctly named destination" in {
    val contracts = Contracts.load()

    val failures = contracts.sourceNames.toList.sorted.flatMap { source =>
      Try(ContractRunner.build(source, TestETLContext(), datasetVersion, rawStorage = "", contracts))
        .failed.toOption.map(e => s"$source -> ${e.getMessage}")
    }

    failures shouldBe empty
  }

  "run" should "publish each MAJOR of a source into its own table" in {
    val List(v1, v2) = tablesOf("fanout")

    runFanOut("fanout")

    onVersionBranch(v1) should contain theSameElementsAs rowsOf(1)
    onVersionBranch(v2) should contain theSameElementsAs rowsOf(2)
  }

  /*
    The point of a MAJOR bump: the tables are independent, so a breaking change cannot reach consumers of
    the previous MAJOR. Asserts absence as well as presence — one shared table would satisfy the test above
    for whichever contract ran last.
  */
  it should "keep the MAJORs isolated from each other" in {
    val List(v1, v2) = tablesOf("isolated")

    runFanOut("isolated")

    onVersionBranch(v1).map(_.major) should contain only 1
    onVersionBranch(v2).map(_.major) should contain only 2
  }

  /*
    The family itself is never written to: it is a template. A table named after it would mean a MAJOR
    published without deriving its destination.
  */
  it should "leave the dataset family's own table uncreated" in {
    runFanOut("template")

    spark.catalog.tableExists("reference.template") shouldBe false
    tablesOf("template").foreach(t => spark.catalog.tableExists(s"reference.$t") shouldBe true)
  }

  it should "leave main empty and no audit branch behind on either table" in {
    runFanOut("branches")

    tablesOf("branches").foreach { table =>
      withClue(s"main of $table must stay empty — consumers read the dataset_version branch: ") {
        spark.table(s"reference.$table").count() shouldBe 0
      }
      refNames(table) should contain allOf ("main", datasetVersion)
      withClue(s"the transient audit branch outlived the import on $table: ") {
        refNames(table).filter(_.startsWith("audit")) shouldBe empty
      }
    }
  }

  /*
    Validation is a plan-wide barrier rather than per contract: one bad row stops the run before the healthy
    contracts write anything, because ContractRunner.build resolves every job before running the first.
  */
  it should "write nothing when one of the declared contracts is unregistered" in {
    val onlyMajorOne: ContractRunner.FactoryLookup = {
      case (source, c) if c.major == 1 => derivingFactories(source)(source, c)
      case _                           => None
    }

    an[IllegalArgumentException] should be thrownBy runFanOut("halfregistered", onlyMajorOne)

    withClue("the registered contract wrote its table before the plan was rejected: ") {
      tablesOf("halfregistered").foreach(t => spark.catalog.tableExists(s"reference.$t") shouldBe false)
    }
  }
}
