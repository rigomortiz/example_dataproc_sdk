package com.bbva.datioamproduct.datahubutils

import com.datio.spark.InitSpark
import com.datio.spark.metric.model.BusinessInformation
import com.typesafe.config.Config
import org.apache.spark.sql.functions.{explode, split}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Main file for Example process.
  * Implements InitSpark which includes metrics and SparkSession.
  *
  * Configuration for this class should be expressed in HOCON like this:
  *
  * Example {
  *   ...
  * }
  *
  */
protected trait ExampleTrait extends InitSpark {
  this: InitSpark =>
  /**
    * @param spark  Initialized SparkSession
    * @param config Config retrieved from args
    */
  override def runProcess(spark: SparkSession, config: Config): Int = {
    this.logger.info("Init process Example")

    var exitCode = 0

    val jobConfig = config.getConfig("Example")
    val inputPath = jobConfig.getString("inputPath")
    val outputPath = jobConfig.getString("outputPath")

    val resultDf = countWords(spark, inputPath)
    resultDf.write.mode(SaveMode.Overwrite).parquet(outputPath)

    exitCode
  }

  def countWords(spark: SparkSession, uri: String): DataFrame = {
    val df = spark.sqlContext.read.text(uri)

    df
      .select(explode(split(df("value"), " ")).alias("word"))
      .groupBy("word")
      .count
  }

  override def defineBusinessInfo(config: Config): BusinessInformation =
    BusinessInformation(exitCode = 0, entity = "", path = "", mode = "",
      schema = "", schemaVersion = "", reprocessing = "")

}

object Example extends ExampleTrait with InitSpark
