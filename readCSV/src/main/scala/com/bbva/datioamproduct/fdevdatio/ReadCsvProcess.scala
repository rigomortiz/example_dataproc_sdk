package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants.{BIKES_CONFIG, CUSTOMER_CONFIG, MESSAGE}
import com.bbva.datioamproduct.fdevdatio.common.StaticValues.{ABR, AMERICA, COUNTRY, MESSAGE_COL, PURCHASE_CONTINENT, PURCHASE_ONLINE}
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, lower}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class ReadCsvProcess extends SparkProcess with IOUtils {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    logger.info("ReadCsvProcess.runProcess")

    Try {
      lazy val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()
      val config: Config = runtimeContext.getConfig
      val message: String = config.getString(MESSAGE)
      logger.info(s"Message: $message")
      logger.info("Read Bikes CSV")
      val bikesConfig = config.getConfig(BIKES_CONFIG)
      val bikesDf:DataFrame = read(bikesConfig)
      bikesDf.printSchema()
      bikesDf.show(false)

      logger.info("Read Customer CSV")
      val customerConfig = config.getConfig(CUSTOMER_CONFIG)
      val customerDf:DataFrame = read(customerConfig)
      customerDf.printSchema()

      val column:Column = lit(message).alias("message")
      val columns: Array[Column] = customerDf.columns.map(s => col(s))
      /*
       +: element concat array
       :+ array concat element
       ++ concat

       Comparaciones
       === igual que
       =!= diferente

       */
      // val lowerCountryColumn = lower(col("country")).alias("abr")
      customerDf
        .select(customerDf.columns.map {
          case name: String if name == COUNTRY => lower(col(COUNTRY)).alias(ABR)
          case _@name => col(name)
        } :+ lit(message).alias(MESSAGE_COL) :_*)
        .filter(col(PURCHASE_CONTINENT) === AMERICA)
        .drop(col(PURCHASE_ONLINE))
        .show(false)


    } match {
      case Success(_) => 0
      case Failure(ex) => {
        ex.printStackTrace()
        -1
      }
    }
  }

  override def getProcessId: String = "ReadCsvProcess"
}