package com.bbva.datioamproduct.fdevdatio.engine

import com.bbva.datioamproduct.fdevdatio.config.{MyConfig, MyConfigHandler}
import com.bbva.datioamproduct.fdevdatio.constants.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.constants.fields.input.Bikes.BikeId
import com.bbva.datioamproduct.fdevdatio.transformations.BikesTransformations.BikesDs
import com.bbva.datioamproduct.fdevdatio.transformations.CustomersBikesTransformations.CustomersBikesDs
import com.bbva.datioamproduct.fdevdatio.transformations.CustomersTransformations.CustomersDs
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.slf4j.LoggerFactory
import com.bbva.datioamproduct.fdevdatio.constants.fields.output.CustomersBikes._

import scala.util.{Failure, Success, Try}

class SparkProcessCourse extends SparkProcess {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val myConfigHandler = new MyConfigHandler()

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    Try {
      logger.info(s"Start SparkProcess: $getProcessId")

      val config: Config = runtimeContext.getConfig
      val myConfig: MyConfig = myConfigHandler.load(config)

      val devName = myConfig.params.getString(DevNameConfig)
      val cutoffDate = myConfig.params.getString(CutoffDateConfig)

      logger.info(s"Developer: $devName")

      val customersDs = myConfig.fdevCustomers.read()
        .filterPurchaseYear()

      val bikesDs = myConfig.fdevBikes.read()
        .filterSizeColumn()

      /**
       * El Sr. Wick requiere conocer la relación entre la compra de la bicicleta y los detalles
       * técnicos de la misma, por lo que se ha decidido realizar un inner join entre ambas tablas.
       */
      customersDs.join(bikesDs, Seq(BikeId.name))
        .addColumns(
          NBikes(),
          TotalSpent(),
          TotalInplace(),
          TotalOnline(),
          IsOnlineCustomer(),
          IsInplaceCustomer(),
          IsHybridCustomer(),
          TotalRefund()
        )
        .show()


    } match {
      case Success(_) => 0
      case Failure(exception) => {
        exception.printStackTrace()
        -1
      }
    }
  }

  override def getProcessId: String = "SparkProcessCourse"

}