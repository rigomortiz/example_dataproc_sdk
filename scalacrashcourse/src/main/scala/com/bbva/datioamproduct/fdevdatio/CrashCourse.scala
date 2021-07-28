package com.bbva.datioamproduct.fdevdatio

import java.net.URI

import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.schema.DatioSchema
import com.typesafe.config.ConfigException
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
  * This example app reads and writes a csv file contained within the project, for external
  * input or output set the environment variables INPUT_PATH or OUTPUT_PATH, or modify the
  * proper configuration paths.
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

      // The DatioSparkSession should be the entrypoint for spark reading/writing
      val datioSparkSession = DatioSparkSession.getOrCreate()

      val jobConfig = context.getConfig.getConfig("CrashCourse").resolve()

      // Schemas can be specified both for reading and writing, please read the docs to find more
      // about their usage (schema of dataset, encryption/decription)
      val inputSchemaPath = jobConfig.getString("inputSchemaPath")
      val outputSchemaPath = jobConfig.getString("outputSchemaPath")
      val inputSchema = DatioSchema.getBuilder.fromURI(URI.create(inputSchemaPath)).build()
      val outputSchema = DatioSchema.getBuilder.fromURI(URI.create(outputSchemaPath)).build()

      // Here is a simple logic that showcases reading and writing using a schema
      val spark = datioSparkSession.getSparkSession
      import spark.implicits._

      val inputPath = jobConfig.getString("inputPath")
      val inputDs = datioSparkSession.read.datioSchema(inputSchema).csv(inputPath)

      val totalMedalsDs = inputDs
          .select($"country",
                  $"total_summer" +
                  $"total_winter" +
                  $"gold_games" +
                  $"silver_games" +
                  $"bronze_games" as "total")

      // Write resulting dataset to console or to a new CSV file
      val outputPath = jobConfig.getString("outputPath")
      if (Option(outputPath).getOrElse("").trim.isEmpty) {
        totalMedalsDs.show(false)
      }
      else {
        datioSparkSession.write.datioSchema(outputSchema).csv(totalMedalsDs, outputPath)
      }
    } match {
      case Success(_) =>
        logger.info("Succesful processing!")
        OK
      case Failure(e) =>
        // You can do wathever exception control you see fit, keep in mind that if the exception
        // bubbles up, it will also be caught at launcher level and the process will return with error
        logger.error("There was an error during the processing of the data", e)
        ERR
    }
  }
}
