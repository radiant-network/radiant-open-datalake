package org.radiant.opendatalake.config.contracts

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import org.radiant.opendatalake.config.contracts.ContractRegistry.Normalizer
import org.slf4j.{Logger, LoggerFactory}

object ContractRunner {

  private val log: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  type FactoryLookup = Contract => Option[NormalizerArgs => Normalizer]

  def plan(source: String, contracts: Contracts, factories: FactoryLookup = ContractRegistry.factory): List[Contract] = {
    val declared = contracts.forSource(source)
    require(declared.nonEmpty, s"No contract declared for source '$source' in contracts.yml")

    val duplicated = declared.groupBy(_.major).collect { case (major, rows) if rows.size > 1 => s"MAJOR $major: ${rows.map(_.lineage).mkString(", ")}"}
    require(duplicated.isEmpty, s"contracts.yml declares the same MAJOR more than once for source '$source'")

    val unregistered = declared.filterNot(c => factories(c).isDefined)
    require(unregistered.isEmpty, s"No contracts entry for source '$source'")

    declared
  }

  def destinationMismatch(contract: Contract, mainDestination: DatasetConf): Option[String] = {
    val actual = mainDestination.table.map(_.name)
    if (actual.contains(contract.table)) None
    else Some(
      s"contract ${contract.lineage} declares table '${contract.table}' but ${mainDestination.id} writes to ${actual.map(t => s"'$t'").getOrElse("no table")}"
    )
  }

  def build(source: String,
            args: NormalizerArgs,
            contracts: Contracts,
            factories: FactoryLookup = ContractRegistry.factory): List[(Contract, Normalizer)] = {

    val jobs = plan(source, contracts, factories).map { c =>
      val factory = factories(c).getOrElse(
        throw new IllegalStateException(s"no factory for table '${c.table}' MAJOR ${c.major} after plan validation")
      )
      c -> factory(args)
    }

    val mismatches = jobs.flatMap { case (c, job) => destinationMismatch(c, job.mainDestination) }
    require(mismatches.isEmpty, s"contracts.yml disagrees with EtlConfiguration: ${mismatches.mkString("; ")}")

    jobs
  }

  def run(source: String,
          rc: RuntimeETLContext,
          version: String,
          rawStorage: String,
          contracts: Contracts = Contracts.load(),
          factories: FactoryLookup = ContractRegistry.factory): Unit = {

    val jobs = build(source, NormalizerArgs(rc, version, rawStorage), contracts, factories)

    log.info(
      s"Fan-out for source '$source' version '$version': ${jobs.size} contract(s) -> " +
        jobs.map { case (c, _) => s"${c.lineage}:${c.table}" }.mkString(", ")
    )

    jobs.foreach { case (c, job) =>
      log.info(s"Running contract ${c.lineage} of '$source' into table '${c.table}' (${job.getClass.getName})")
      job.run()
    }
  }
}
