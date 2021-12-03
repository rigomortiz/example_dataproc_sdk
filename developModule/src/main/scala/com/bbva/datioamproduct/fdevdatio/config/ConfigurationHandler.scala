package com.bbva.datioamproduct.fdevdatio.config

import com.bbva.datioamproduct.fdevdatio.Engine
import com.bbva.datioamproduct.fdevdatio.constants.ConfigConstants._
import com.datio.dataproc.kirby.api.{Input, Output}
import com.datio.dataproc.kirby.core.actions.{DynamicDatasetSchema, Encrypt, InternalKirbyAction, Repartition, ReprocessPartitions, SafeReprocess, Validation}
import com.datio.dataproc.kirby.core.config.configurable.KirbyConfigurableFactory
import com.datio.dataproc.kirby.core.config.exception.MissingMandatoryOutputSchemaException
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class ConfigurationHandler() {
  private val logger = LoggerFactory.getLogger(classOf[Engine])
  private val inputFactory = new KirbyConfigurableFactory[Input]()
  private val outputFactory = new KirbyConfigurableFactory[Output]()

  def load(rootConfig: Config): JobConfig = {
    logger.info("Load config structure")

    val customersPhonesConfig: Config = rootConfig.getConfig(CUSTOMERS_PHONES_CONFIG)
    val fDevCustomersPhonesInternalActions = getPreOutputInternalActions(customersPhonesConfig)
    val fDevCustomersPhones = outputFactory.getImplementation(fDevCustomersPhonesInternalActions
      .foldLeft(customersPhonesConfig)((c, a) => a.prepareConfig(c)))

    JobConfig(
      params = rootConfig.getConfig(PARAMS_CONFIG),
      fDevCustomers = inputFactory.getImplementation(rootConfig.getConfig(CUSTOMERS_CONFIG)),
      fDevPhones = inputFactory.getImplementation(rootConfig.getConfig(PHONES_CONFIG)),
      fDevCustomersPhonesInternalActions = fDevCustomersPhonesInternalActions,
      fDevCustomersPhones = fDevCustomersPhones
    )
  }

  private def getPreOutputInternalActions(config: Config): Seq[InternalKirbyAction] = {
    val safeReprocess = SafeReprocess(config)
    val schema = Some(Option(outputFactory.getImplementation(config).getSchema.orElse(None.orNull))
      .getOrElse(throw MissingMandatoryOutputSchemaException()))

    Seq(Encrypt(schema, safeReprocess, None.orNull),
      safeReprocess.prepareOutputAction(),
      Repartition(config),
      DynamicDatasetSchema(config, schema),
      Validation(schema),
      ReprocessPartitions(config))
  }

}
