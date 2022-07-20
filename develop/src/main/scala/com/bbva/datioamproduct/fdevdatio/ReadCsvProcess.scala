package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants.{BIKES_CONFIG, CUSTOMER_BIKES_CONFIG, CUSTOMER_CONFIG,
  MESSAGE}
import com.bbva.datioamproduct.fdevdatio.common.namings.output.CustomerBikes.{IsHybridCustomer, IsInPlaceCustomer,
  IsOnlineCustomer, NBikes, TotalInPlace, TotalOnline, TotalRefund, TotalSpent}
import com.bbva.datioamproduct.fdevdatio.transformations.Transformations.{CustomerBikesDf, CustomerDf, BikesDf}
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class ReadCsvProcess extends SparkProcess with IOUtils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    logger.info(">>> ReadCsvProcess.runProcess")

    Try {
      // lazy val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()
      val config: Config = runtimeContext.getConfig
      val message: String = config.getString(MESSAGE)
      logger.info(s">>> Message: $message")
      logger.info(">>> Read Bikes CSV")
      val bikesConfig = config.getConfig(BIKES_CONFIG)
      val bikesDf:DataFrame = read(bikesConfig)
      bikesDf.printSchema()
      val outputBikesDf = bikesDf.filterSize()
      outputBikesDf.show(numRows = 50, truncate = false)

      logger.info(">>> Read Customer CSV")
      val customerConfig = config.getConfig(CUSTOMER_CONFIG)
      val customerDf:DataFrame = read(customerConfig)
      customerDf.printSchema()

      val outputCustomerDF: DataFrame = customerDf
        .addMessage(message)
        .lowerCountry()
        .filterYearAndCity()
        .joinBikes(outputBikesDf)
        .replacePurchaseCity
        .addColumns(
          NBikes(),
          TotalSpent(),
          TotalOnline(),
          TotalInPlace(),
          IsInPlaceCustomer(),
          IsOnlineCustomer(),
          IsHybridCustomer(),
          TotalRefund()
        )

      outputCustomerDF.show(numRows = 50, truncate = false)

      logger.info(">>> Save output")
      val customerBikesConfig: Config = config.getConfig(CUSTOMER_BIKES_CONFIG)
      outputCustomerDF.show(numRows = 50, truncate = false)
      write(outputCustomerDF, customerBikesConfig)

    } match {
      case Success(_) => 0
      case Failure(exception: InvalidDatasetException) => {
        exception.getErrors.toArray.foreach {
          err => logger.error(err.toString)
        }
        -1
      }
      case Failure(ex) => {
        ex.printStackTrace()
        -1
      }
    }
  }

  override def getProcessId: String = "ReadCsvProcess"
}