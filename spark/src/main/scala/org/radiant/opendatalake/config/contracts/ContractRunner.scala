package org.radiant.opendatalake.config.contracts

import bio.ferlab.datalake.commons.config.{DatasetConf, RuntimeETLContext}
import org.radiant.opendatalake.config.contracts.ContractRegistry.Normalizer
import org.slf4j.{Logger, LoggerFactory}

object ContractRunner {

  private val log: Logger = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  type FactoryLookup = String => Option[NormalizerArgs => Normalizer]

  def plan(source: String, contracts: Contracts, factories: FactoryLookup = ContractRegistry.factory): List[Contract] = {
    val declared = contracts.forSource(source)

    require(declared.nonEmpty, s"No contract declared for source '$source' in contracts.yml (declared sources: " + s"${contracts.sourceNames.toList.sorted.mkString(", ")})")

    // One row per MAJOR is the file's core invariant: two rows sharing a MAJOR would fan out to the
    // same table twice, the second run overwriting the first.
    val duplicated = declared.groupBy(_.major).collect {
      case (major, rows) if rows.size > 1 => s"MAJOR $major: ${rows.map(_.lineage).mkString(", ")}"
    }
    require(
      duplicated.isEmpty,
      s"contracts.yml declares the same MAJOR more than once for source '$source' " +
        s"(${duplicated.toList.sorted.mkString("; ")}); a new MAJOR is a new row, a new MINOR bumps " +
        s"the lineage of the existing row"
    )

    val unknown = declared.filterNot(c => factories(c.normalizer).isDefined)
    require(
      unknown.isEmpty,
      s"contracts.yml declares normalizer(s) absent from ContractRegistry for source '$source': " +
        s"${unknown.map(c => s"${c.lineage} -> ${c.normalizer}").mkString(", ")}"
    )

    declared
  }

  def destinationMismatch(contract: Contract, mainDestination: DatasetConf): Option[String] =
    mainDestination.table.map(_.name) match {
      case Some(actual) if actual == contract.table => None
      case Some(actual) =>
        Some(
          s"contract ${contract.lineage} declares table '${contract.table}' but " +
            s"${contract.normalizer} writes to '$actual'"
        )
      case None =>
        Some(
          s"contract ${contract.lineage} declares table '${contract.table}' but " +
            s"${contract.normalizer} has no table in its destination (${mainDestination.id})"
        )
    }

  def destinationMismatches(destinations: List[(Contract, DatasetConf)]): List[String] =
    destinations.flatMap { case (contract, destination) => destinationMismatch(contract, destination) }

  def build(source: String,
            args: NormalizerArgs,
            contracts: Contracts,
            factories: FactoryLookup = ContractRegistry.factory): List[(Contract, Normalizer)] = {

    val jobs = plan(source, contracts, factories).map { c =>
      val factory = factories(c.normalizer).getOrElse(
        throw new IllegalStateException(s"no factory for ${c.normalizer} after plan validation")
      )
      c -> factory(args)
    }

    val mismatches = destinationMismatches(jobs.map { case (c, job) => c -> job.mainDestination })
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
      log.info(s"Running contract ${c.lineage} of '$source' (${c.normalizer}) into table '${c.table}'")
      job.run()
      log.info(s"Contract ${c.lineage} of '$source' completed")
    }
  }
}
