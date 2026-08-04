package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.Format.ICEBERG
import bio.ferlab.datalake.commons.config.LoadType.OverWrite
import bio.ferlab.datalake.commons.config.{DatasetConf, RunStep, RuntimeETLContext, TableConf}
import bio.ferlab.datalake.testutils.TestETLContext
import org.apache.spark.sql.DataFrame
import org.radiant.opendatalake.config.Contracts
import org.radiant.opendatalake.testutils.{CreateDatabasesBeforeAll, SparkSpec}
import org.radiant.opendatalake.wap.WapETLP

import java.time.LocalDateTime
import scala.util.Try

case class DemoRow(id: String, major: Int)

class ContractFanOutSpec extends SparkSpec with CreateDatabasesBeforeAll {

  import spark.implicits._

  override val dbToCreate: List[String] = List("reference")

  private val datasetVersion = "20260715"

  private def familyOf(source: String): DatasetConf =
    DatasetConf(
      id = s"normalized_$source",
      storageid = "iceberg_storage",
      path = s"/$source",
      format = ICEBERG,
      loadtype = OverWrite,
      table = Some(TableConf("reference", source))
    )

  private def prefixOf(source: String): String = s"${source}_open"

  private def tablesOf(source: String): List[String] =
    List(1, 2).map(ContractDestination.tableName(prefixOf(source), _))

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

  private def derivingFactories(source: String): ContractRunner.FactoryLookup = (_, contract) =>
    Some { args =>
      val destination = ContractDestination.forMajor(familyOf(source), args.tablePrefix, contract.major)
      FixedRowsNormalizer(args.rc, args.version, destination, rowsOf(contract.major).toDF())
    }

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

  it should "keep the MAJORs isolated from each other" in {
    val List(v1, v2) = tablesOf("isolated")

    runFanOut("isolated")

    onVersionBranch(v1).map(_.major) should contain only 1
    onVersionBranch(v2).map(_.major) should contain only 2
  }

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
