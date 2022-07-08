package com.bbva.datioamproduct.fdevdatio

import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

class HelloWorldProcess extends SparkProcess {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  override def runProcess(runtimeContext: RuntimeContext): Int = {
    logger.info("HelloWorldProcess.runProcess")
    Try {
      val config: Config = runtimeContext.getConfig
      val message: String = config.getString("HelloWorld.params.message")
      logger.info(s"Message: $message")
    } match {
      case Success(_) => 0
      case Failure(ex) => {
        ex.printStackTrace()
        -1
      }
    }
  }

  override def getProcessId: String = "HelloWorldProcess"
}