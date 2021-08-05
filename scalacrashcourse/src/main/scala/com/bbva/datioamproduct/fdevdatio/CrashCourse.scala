package com.bbva.datioamproduct.fdevdatio

import java.net.URI

import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.schema.DatioSchema
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

/**
  * Main entrypoint for CrashCourse process.
  * Implements SparkProcess so it can be found by the Dataproc launcher using JSPI.
  *
  * Configuration for this class should be expressed in HOCON like this:
  *
  * CrashCourse {
  *   ...
  * }
  *
  */
class CrashCourse extends SparkProcess {

  private val logger = LoggerFactory.getLogger(classOf[CrashCourse])

  override def getProcessId: String = "CrashCourse"

  override def runProcess(context: RuntimeContext): Int = {

    val OK = 0
    val ERR = 1

    Try {

      logger.info(s"Process Id: ${context.getProcessId}")

    } match {
      case Success(_) =>
        logger.info("Successful processing!")
        OK
      case Failure(e) =>
        logger.error("There was an error during the processing of the data", e)
        ERR
    }
  }
}
