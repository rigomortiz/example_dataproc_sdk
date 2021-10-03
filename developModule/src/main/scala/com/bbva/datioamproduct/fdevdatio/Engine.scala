package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants
import com.bbva.datioamproduct.fdevdatio.common.StaticVals.JoinTypes
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Phones.{CustomerId, DeliveryId}
import com.bbva.datioamproduct.fdevdatio.common.namings.output.CustomersPhones.{Age, CustomerVip, DiscountExtra, FinalPrice, JwkDate}
import com.bbva.datioamproduct.fdevdatio.transformations.Transformations._
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

class Engine extends SparkProcess with LazyLogging {

  val OK: Int = 0
  val ERR: Int = -1

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    Try {
      logger.info(s"Process Id: ${runtimeContext.getProcessId}")
      val datioSparkSession = DatioSparkSession.getOrCreate()
      val config: Config = runtimeContext.getConfig

      val jwkDate: String = config.getString(ConfigConstants.JWK_DATE)

      //Load inputs
      val phonesPath: String = config.getString(ConfigConstants.PHONES_PATH)
      val phonesDF: PhonesTransformer = datioSparkSession.read().parquet(phonesPath)
      val customersPath: String = config.getString(ConfigConstants.CUSTOMERS_PATH)
      val customersDF: CustomerTransformer = datioSparkSession.read().parquet(customersPath)

      // Regla 1, 2, 3
      val customerPhonesDF: CustomerPhonesTransformer = phonesDF.filterPhones().join(
        customersDF.filterCustomer(),
        Seq(CustomerId.name, DeliveryId.name),
        JoinTypes.INNER
      )

      customerPhonesDF
        .addColumn(CustomerVip()) //Regla 4
        .addColumn(DiscountExtra()) //Regla 5
        .addColumn(FinalPrice()) // Regla 6
        .addColumn(Age()) //Regla 7
        .filterBrandsTop() //Regla 8
        .addColumn(JwkDate(jwkDate)) //Regla 9
        .cleanNfcColumn() //Regla 10
        .show()

      /*
      val schemaPath = config.getString(ConfigConstants.OUTPUT_SCHEMA)
      val outputPath = config.getString(ConfigConstants.OUTPUT_PATH)
      val outputSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
      datioSparkSession
        .write()
        .mode(SaveMode.Overwrite)
        .datioSchema(outputSchema)
        .partitionBy("country", "year")
        .parquet(unionDf, outputPath)
       */

    } match {
      case Success(_) => OK
      case Failure(exception: Exception) => {
        exception.printStackTrace()
        ERR
      }
    }
  }

  override def getProcessId: String = "Engine"
}