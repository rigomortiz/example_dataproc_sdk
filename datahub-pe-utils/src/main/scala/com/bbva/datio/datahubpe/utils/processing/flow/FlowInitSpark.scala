package com.bbva.datio.datahubpe.utils.processing.flow

import com.bbva.datio.datahubpe.utils.processing.data.{DataReader, DataWriter}
import com.bbva.datio.datahubpe.utils.processing.flow.impl.{ConcreteReader, ConcreteWriter}
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.schema.exception.DataprocSchemaException.InvalidDatasetException
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

abstract class FlowInitSpark extends SparkProcess {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  final override def runProcess(runtimeContext: RuntimeContext): Int = {
    Try {
      val config: Config = runtimeContext.getConfig

      logger.info("Reading input")
      val concreteReader = getReader(config)
      val dataReader = concreteReader.read()

      logger.info("Apply transformations")
      val dataWriter = getTransformer(config).transform(dataReader)

      logger.info("Apply writter")
      val concreteWriter = new ConcreteWriter(config)
      concreteWriter.write(dataWriter)

    } match {
      case Success(_) => 0
      case Failure(exception: InvalidDatasetException) => {
        for (err <- exception.getErrors.toArray) {
          logger.error(err.toString)
        }
        -1
      }
      case Failure(exception: Exception) => {
        exception.printStackTrace()
        -1
      }
    }
  }

  def getTransformer(config: Config): Transformer[DataReader, DataWriter]

  protected def getReader(config: Config): Reader[DataReader] = new ConcreteReader(config)

  override def getProcessId: String
}
