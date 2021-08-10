package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.common.StaticVal
import com.bbva.datioamproduct.fdevdatio.input.DataframeFilter
import com.bbva.datioamproduct.fdevdatio.transformations.YoutubeAnalytics

import java.net.URI
import com.datio.dataproc.sdk.api.SparkProcess
import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.schema.DatioSchema
import org.apache.spark.sql.SaveMode
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
      val datioSparkSession = DatioSparkSession.getOrCreate()

      /**
       * PARAMETERS
       */
      val config = context.getConfig
      val country = config.getString(StaticVal.PARAMETERS_COUNTRY)
      val top = config.getInt(StaticVal.PARAMETERS_TOP)
      val year = config.getInt(StaticVal.PARAMETERS_YEAR)

      /**
       * INPUTS
       */
      val channelsTable = config.getString(StaticVal.CHANNELS_TABLE)
      val allChannels = datioSparkSession.read().parquet(channelsTable)
      val filteredChannels = DataframeFilter.getChannelsTable(allChannels, country)

      val videoInfo = config.getString(StaticVal.VIDEO_INFO)
      val allVideos = datioSparkSession.read().parquet(videoInfo)
      val infoVideos = DataframeFilter.getVideoInfo(allVideos, year)

      /**
       * TRANSFORMATIONS
       */

      val topChannels = YoutubeAnalytics.joinChannelsTop(filteredChannels, infoVideos).persist()

      val mostViewed = YoutubeAnalytics.createTopCategory(topChannels, StaticVal.MOST_VIEWED, top)
      val mostLiked = YoutubeAnalytics.createTopCategory(topChannels, StaticVal.MOST_LIKED, top)
      val mostHated = YoutubeAnalytics.createTopCategory(topChannels, StaticVal.MOST_HATED, top)

      val unionDf = mostViewed.union(mostLiked).union(mostHated)

      /**
       * OUTPUTS
       */
      val schemaPath = config.getString(StaticVal.OUTPUT_SCHEMA)
      val outputPath = config.getString(StaticVal.OUTPUT_PATH)
      val outputSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
      datioSparkSession
        .write()
        .mode(SaveMode.Overwrite)
        .datioSchema(outputSchema)
        .partitionBy("country", "year")
        .parquet(unionDf, outputPath)
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
