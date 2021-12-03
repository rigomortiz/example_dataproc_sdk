package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.config.{JobConfig, ConfigurationHandler}
import com.bbva.datioamproduct.fdevdatio.constants.ConfigConstants
import com.bbva.datioamproduct.fdevdatio.constants.StaticVals.JoinTypes
import com.bbva.datioamproduct.fdevdatio.constants.namings.input.Phones.{CustomerId, DeliveryId}
import com.bbva.datioamproduct.fdevdatio.constants.namings.output.CustomersPhones._
import com.bbva.datioamproduct.fdevdatio.transformations.Transformations._
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.sql.functions.lit

import scala.util.{Failure, Success, Try}

class Engine extends SparkProcess {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val configurationHandler = new ConfigurationHandler()

  private val OK: Int = 0
  private val ERR: Int = -1

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    Try {
      logger.info(s"Process Id: ${runtimeContext.getProcessId}")
      val config: Config = runtimeContext.getConfig
      val jobConfig: JobConfig = configurationHandler.load(config)
      val jwkDate: String = config.getString(ConfigConstants.JWK_DATE)

      logger.info("Load inputs")
      //Load inputs
      val phonesDs: PhonesTransformer = jobConfig.fDevPhones.read()
      val customersDs: CustomersTransformer = jobConfig.fDevCustomers.read()

      logger.info("Apply transformations")
      // Regla 1, 2, 3
      val customerPhonesDs: CustomersPhonesTransformer = phonesDs
        .filterPhones()
        .join(customersDs.filterCustomers(),
          Seq(CustomerId.name, DeliveryId.name),
          JoinTypes.INNER
        )

      val transformedDs: DataFrame = customerPhonesDs
        .addColumn(CustomerVip()) //Regla 4
        .addColumn(ExtraDiscount()) //Regla 5
        .addColumn(FinalPrice()) // Regla 6
        .addColumn(Age()) //Regla 7
        .filterBrandsTop() //Regla 8
        .addColumn(JwkDate(jwkDate)) //Regla 9
        .cleanNfcColumn() //Regla 10
        .fitToSchema() // Selecciona únicamente las columnas que el esquema indica
        .withColumn("extra", lit(100))


      logger.debug("Apply pre output internal actions") //Es necesario para escribir en un sólo archivo y validar esquema
      val outputDs = jobConfig.fDevCustomersPhonesInternalActions.foldLeft(transformedDs)((ds, action) => action.doAction(ds))
      logger.info("Write the output")
      jobConfig.fDevCustomersPhones.write(outputDs)

    } match {
      case Success(_) => OK
      case Failure(exception: InvalidDatasetException) => {
        for (err <- exception.getErrors.toArray) {
          logger.error(err.toString)
        }
        ERR
      }
      case Failure(exception: Exception) => {
        exception.printStackTrace()
        ERR
      }
    }
  }

  override def getProcessId: String = "Engine"
}