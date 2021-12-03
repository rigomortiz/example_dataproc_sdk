package com.bbva.datioamproduct.fdevdatio.config


import com.datio.dataproc.kirby.api.{Input, Output}
import com.datio.dataproc.kirby.core.actions.InternalKirbyAction
import com.typesafe.config.Config

case class JobConfig(params: Config,
                     fDevCustomers: Input,
                     fDevPhones: Input,
                     fDevCustomersPhonesInternalActions: Seq[InternalKirbyAction] = Seq(),
                     fDevCustomersPhones: Output
                    )
