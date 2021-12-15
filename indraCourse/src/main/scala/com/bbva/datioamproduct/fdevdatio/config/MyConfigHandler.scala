package com.bbva.datioamproduct.fdevdatio.config

import com.datio.dataproc.kirby.api.{Input, Output}
import com.datio.dataproc.kirby.core.config.configurable.KirbyConfigurableFactory
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import com.bbva.datioamproduct.fdevdatio.constants.ConfigConstants._

class MyConfigHandler {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val inputFactory = new KirbyConfigurableFactory[Input]
  private val outputFactory = new KirbyConfigurableFactory[Output]

  def load(config: Config): MyConfig = {
    logger.info("Loading Config")

    val customersConfig = config.getConfig(FdevCustomersConfig)
    val bikesConfig = config.getConfig(FdevBikesConfig)
    val paramsConfig = config.getConfig(ParamsConfig)

    MyConfig(
      fdevCustomers = inputFactory.getImplementation(customersConfig),
      fdevBikes = inputFactory.getImplementation(bikesConfig),
      params = paramsConfig
    )
  }

}
