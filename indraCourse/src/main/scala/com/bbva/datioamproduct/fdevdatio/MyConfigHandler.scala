package com.bbva.datioamproduct.fdevdatio.config

import com.datio.dataproc.kirby.api.{Input, Output}
import com.datio.dataproc.kirby.core.config.configurable.KirbyConfigurableFactory
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

class MyConfigHandler {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val inputFactory = new KirbyConfigurableFactory[Input]
  private val outputFactory = new KirbyConfigurableFactory[Output]

  def load(config: Config): MyConfig = {
    logger.info("Loading Config")

    val customersConfig = config.getConfig("2021q4g3.inputs.fdevCustomers")
    val paramsConfig = config.getConfig("2021q4g3.params")

    MyConfig(
      fdevCustomers = inputFactory.getImplementation(customersConfig),
      params = paramsConfig
    )

  }

}
