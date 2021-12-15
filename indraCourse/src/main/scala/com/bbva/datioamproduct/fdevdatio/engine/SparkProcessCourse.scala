package com.bbva.datioamproduct.fdevdatio.engine

import com.bbva.datioamproduct.fdevdatio.config.{MyConfig, MyConfigHandler}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}
import com.bbva.datioamproduct.fdevdatio.constants.ConfigConstants.DevNameConfig

class SparkProcessCourse extends SparkProcess {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val myConfigHandler = new MyConfigHandler()

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    Try {
      logger.info(s"Start SparkProcess: $getProcessId")

      val config: Config = runtimeContext.getConfig
      val myConfig: MyConfig = myConfigHandler.load(config)

      val devName = myConfig.params.getString(DevNameConfig)

      logger.info(s"Developer: $devName")

      val ds: Dataset[Row] = myConfig.fdevCustomers.read()
      ds.show(20, false)

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
