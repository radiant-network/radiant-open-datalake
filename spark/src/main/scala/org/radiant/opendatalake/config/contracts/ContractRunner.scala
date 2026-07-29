package org.radiant.opendatalake.config.contracts

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import org.radiant.opendatalake.config.contracts.ContractRegistry.Normalizer
import org.slf4j.{Logger, LoggerFactory}

object ContractRunner {

  private val log: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  type FactoryLookup = Contract => Option[NormalizerArgs => Normalizer]

  /*
    Returns the resolved factory alongside each contract rather than the contract alone: `build` would
    otherwise repeat the lookup and need an unreachable branch for the None the checks below rule out.
  */
  def plan(source: String,
           contracts: Contracts,
           factories: FactoryLookup = ContractRegistry.factory): List[(Contract, NormalizerArgs => Normalizer)] = {

    val declared = contracts.forSource(source)
    require(declared.nonEmpty, s"No contract declared for source '$source' in contracts.yml")

    val duplicated = declared.groupBy(_.major).collect { case (major, rows) if rows.size > 1 => s"MAJOR $major: ${rows.map(_.lineage).mkString(", ")}"}
    require(duplicated.isEmpty, s"contracts.yml declares the same MAJOR more than once for source '$source': ${duplicated.mkString("; ")}")

    val resolved = declared.map(c => c -> factories(c))
    val unregistered = resolved.collect { case (c, None) => s"${c.lineage} -> (${c.table}, MAJOR ${c.major})" }
    require(unregistered.isEmpty, s"No ContractRegistry entry for source '$source': ${unregistered.mkString(", ")}")

    resolved.collect { case (c, Some(factory)) => c -> factory }
  }

  def destinationMismatch(contract: Contract, mainDestination: DatasetConf): Option[String] = {
    // Annotated: this is Option.contains (exact match on the element), not String.contains (substring).
    val actual: Option[String] = mainDestination.table.map(_.name)
    if (actual.contains(contract.table)) None
    else Some(
      s"contract ${contract.lineage} declares table '${contract.table}' but ${mainDestination.id} writes to ${actual.map(t => s"'$t'").getOrElse("no table")}"
    )
  }

  def build(source: String,
            args: NormalizerArgs,
            contracts: Contracts,
            factories: FactoryLookup = ContractRegistry.factory): List[(Contract, Normalizer)] = {

    val jobs = plan(source, contracts, factories).map { case (c, factory) => c -> factory(args) }

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
