package com.bbva.datioamproduct.fdevdatio.config

import com.datio.dataproc.kirby.api.Input
import com.typesafe.config.{Config, ConfigFactory}

case class MyConfig(
                     params: Config = ConfigFactory.defaultApplication(),
                     fdevCustomers: Input,
                     fdevBikes: Input
                   )
