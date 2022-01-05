package com.bbva.datioamproduct.fdevdatio.config

import com.datio.dataproc.kirby.api.{Input, Output}
import com.datio.dataproc.kirby.core.config.configurable.KirbyConfigurableFactory
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import com.bbva.datioamproduct.fdevdatio.constants.ConfigConstants._
import com.datio.dataproc.kirby.core.actions.{DynamicDatasetSchema, Encrypt, InternalKirbyAction, Repartition, ReprocessPartitions, SafeReprocess, Validation}
import com.datio.dataproc.kirby.core.config.exception.MissingMandatoryOutputSchemaException

class MyConfigHandler {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val inputFactory = new KirbyConfigurableFactory[Input]
  private val outputFactory = new KirbyConfigurableFactory[Output]

  def load(config: Config): MyConfig = {
    logger.info("Loading Config")

    val customersConfig = config.getConfig(FdevCustomersConfig)
    val bikesConfig = config.getConfig(FdevBikesConfig)
    val paramsConfig = config.getConfig(ParamsConfig)


    val customersBikesConfig = config.getConfig(FdevCustomersBikesConfig)
    val fDevCustomersBikesInternalActions = getPreOutputInternalActions(customersBikesConfig)
    val fDevCustomersPhones = outputFactory.getImplementation(fDevCustomersBikesInternalActions
      .foldLeft(customersBikesConfig)((c, a) => a.prepareConfig(c)))

    MyConfig(
      fdevCustomers = inputFactory.getImplementation(customersConfig),
      fdevBikes = inputFactory.getImplementation(bikesConfig),
      params = paramsConfig,
      fdevCustomersBikes = fDevCustomersPhones,
      fdevCustomersBikesInternalActions = fDevCustomersBikesInternalActions
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
