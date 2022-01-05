package com.bbva.datioamproduct.fdevdatio.config

import com.datio.dataproc.kirby.api.{Input, Output}
import com.datio.dataproc.kirby.core.actions.InternalKirbyAction
import com.typesafe.config.{Config, ConfigFactory}

case class MyConfig(
                     params: Config = ConfigFactory.defaultApplication(),
                     fdevCustomers: Input,
                     fdevBikes: Input,
                     fdevCustomersBikes: Output,
                     fdevCustomersBikesInternalActions: Seq[InternalKirbyAction]
                   )
