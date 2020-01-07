package com.bbva.datio.datahubpe.utils.processing.flow

import com.bbva.datio.datahubpe.utils.processing.data.{DataReader, DataWriter}
import com.bbva.datio.datahubpe.utils.processing.flow.impl.{ConcreteReader, ConcreteSchemaValidator, ConcreteWriter}
import com.datio.spark.metric.model.BusinessInformation
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession


trait FlowInitSpark extends scala.AnyRef with com.datio.spark.SparkLauncher {


  override final def runProcess(sparkT: SparkSession, config: Config): Int = {

    implicit val spark: SparkSession = sparkT

    logger.info("Reading input")
    val concreteReader = new ConcreteReader(spark, config)
    val dataReader = concreteReader.read()

    logger.info("Apply transformations")
    val dataWriter = getTransformer().transform(dataReader)

    logger.info("Apply Validation")
    val concreteSchemaValidator = new ConcreteSchemaValidator(spark, config)
    val dataWriterValidate = concreteSchemaValidator.validate(dataWriter)

    logger.info("Apply writter")
    val concreteWriter = new ConcreteWriter(spark, config)
    concreteWriter.write(dataWriterValidate)

    var exitCode = 0
    exitCode
  }


  override final def defineBusinessInfo(config: Config): BusinessInformation =
    BusinessInformation(exitCode = 0, entity = "", path = "", mode = "",
      schema = "", schemaVersion = "", reprocessing = "")


  final def main(args: Array[String]): Unit = {

    var exitCode = 0

    try {
      exitCode = runTask(args)
    }
    finally {
      logger.info(s"System Exit Code: $exitCode")
      System.exit(exitCode)
    }
  }

  def getTransformer(): Transformer[DataReader, DataWriter]
}
