package com.bbva.datioamproduct.fdevdatio

import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.apache.spark.sql.{Dataset, Row}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class Engine2021Q4G3 extends SparkProcess {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val myConfigHandler = new MyConfigHandler()

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    Try {
      logger.info(s"Start SparkProcess: $getProcessId")

      val config: Config = runtimeContext.getConfig
      val myConfig: MyConfig = myConfigHandler.load(config)

      val ds: Dataset[Row] = myConfig.fdevCustomers.read()

      ds.show(20)

    } match {
      case Success(_) => 0
      case Failure(exception) => {
        exception.printStackTrace()
        -1
      }
    }
  }

  override def getProcessId: String = "Engine2021Q4G3"
}
