package org.radiant.opendatalake.contracts

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import org.radiant.opendatalake.config.{Contract, Contracts}
import org.radiant.opendatalake.contracts.ContractRegistry.NormalizerETL
import org.slf4j.{Logger, LoggerFactory}

case class ContractPlan(tablePrefix: String, jobs: List[(Contract, NormalizerArgs => NormalizerETL)])

object ContractRunner {

  private val log: Logger = LoggerFactory.getLogger(getClass.getCanonicalName)

  type FactoryLookup = (String, Contract) => Option[NormalizerArgs => NormalizerETL]

  private val MajorSuffix = """_v\d+$""".r

  def plan(source: String,
           contracts: Contracts,
           factories: FactoryLookup = ContractRegistry.factory): ContractPlan = {

    val tablePrefix = contracts.tablePrefixOf(source).getOrElse(
      throw new IllegalArgumentException(s"No contract declared for source '$source' in contracts.yml")
    )

    val declared = contracts.forSource(source)
    require(declared.nonEmpty, s"No contract declared for source '$source' in contracts.yml")

    require(MajorSuffix.findFirstIn(tablePrefix).isEmpty, s"table_prefix '$tablePrefix' already carries a MAJOR suffix; declare it without the suffix (MAJOR comes from the lineage)")

    val duplicatedMajors = declared
      .groupBy(_.major)
      .collect {
        case (major, rows) if rows.size > 1
        => s"MAJOR $major: ${rows.map(_.lineage).mkString(", ")}"
      }

    require(
      duplicatedMajors.isEmpty,
      s"contracts.yml declares the same MAJOR more than once for source '$source': ${duplicatedMajors.mkString("; ")}"
    )

    val resolved = declared.map(c => c -> factories(source, c))
    val unregistered = resolved.collect {
      case (c, None) => s"${c.lineage} -> ($source, MAJOR ${c.major})"
    }

    require(
      unregistered.isEmpty,
      s"No ContractRegistry entry for source '$source': ${unregistered.mkString(", ")}"
    )

    ContractPlan(tablePrefix, resolved.collect { case (c, Some(factory)) => c -> factory })
  }

  def destinationMismatchReason(tablePrefix: String, contract: Contract, mainDestination: DatasetConf): Option[String] = {
    val expected = ContractDestination.tableName(tablePrefix, contract.major)

    mainDestination.table.map(_.name) match {
      case Some(name) if name == expected => None
      case Some(name) => Some(s"contract ${contract.lineage} publishes to '$expected' but ${mainDestination.id} writes to '$name'")
      case None       => Some(s"contract ${contract.lineage} publishes to '$expected' but ${mainDestination.id} writes to no table")
    }
  }

  def build(source: String,
            rc: RuntimeETLContext,
            version: String,
            rawStorage: String,
            contracts: Contracts,
            factories: FactoryLookup = ContractRegistry.factory,
            database: Option[String] = None,
            warehouse: Option[String] = None): List[(Contract, NormalizerETL)] = {

    val contractPlan = plan(source, contracts, factories)
    val args = NormalizerArgs(rc, version, rawStorage, contractPlan.tablePrefix, database, warehouse)
    val jobs = contractPlan.jobs.map { case (c, factory) => c -> factory(args) }

    val mismatches = jobs.flatMap {
      case (c, job) =>
        destinationMismatchReason(contractPlan.tablePrefix, c, job.mainDestination)
    }
    require(mismatches.isEmpty, s"a normalizer does not write to the table its contract implies: ${mismatches.mkString("; ")}")

    jobs
  }

  def run(source: String,
          rc: RuntimeETLContext,
          version: String,
          rawStorage: String,
          contracts: Contracts = Contracts.load(),
          factories: FactoryLookup = ContractRegistry.factory,
          database: Option[String] = None,
          warehouse: Option[String] = None): Unit = {

    val jobs = build(source, rc, version, rawStorage, contracts, factories, database, warehouse)
    
    jobs.foreach { case (c, job) =>
      log.info(s"Running contract ${c.lineage} of '$source' into table '${destinationTableOf(job)}' (${job.getClass.getName}) for version '$version'")
      job.run()
    }
  }

  private def destinationTableOf(job: NormalizerETL): String = job.mainDestination.table.fold("<no table>")(_.name)
}
