package com.bbva.datioamproduct.utils.flow


import com.bbva.datioamproduct.utils.data.{DataReader, DataWriter}
import com.bbva.datioamproduct.utils.flow.impl.{ConcreteReader, ConcreteWriter}
import com.datio.spark.metric.model.BusinessInformation
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession


trait FlowInitSpark extends scala.AnyRef with com.datio.spark.SparkLauncher {


  override final def runProcess(sparkT: SparkSession, config: Config): Int = {

    implicit val spark: SparkSession = sparkT

    val globalConfig = config.getConfig("CptInvoicesJob")

    logger.info("Reading input")
    val concreteReader = new ConcreteReader(spark, config)
    val dataReader = concreteReader.read()

    logger.info("Apply transformations")
    val dataWriter = getTranformer().transform(dataReader)

    logger.info("Apply writter")
    val concreteWriter = new ConcreteWriter(spark,config)
    concreteWriter.write(dataWriter)

    var exitCode = 0
    exitCode
  }



  override final def defineBusinessInfo(config: Config): BusinessInformation =
    BusinessInformation(exitCode = 0, entity = "", path = "", mode = "",
                        schema = "", schemaVersion = "", reprocessing = "")



  final def  main(args: Array[String]): Unit = {

    var exitCode = 0

    try {
      exitCode = runTask(args)
    }
    finally {
      logger.info(s"System Exit Code: $exitCode")
      System.exit(exitCode)
    }
  }

  def getTranformer() : Transformer[DataReader,DataWriter]
}
