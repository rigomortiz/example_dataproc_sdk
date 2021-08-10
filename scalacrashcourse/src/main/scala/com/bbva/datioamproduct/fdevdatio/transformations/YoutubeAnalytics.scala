package com.bbva.datioamproduct.fdevdatio.transformations

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.expressions.Window
import com.bbva.datioamproduct.fdevdatio.common.Columns._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.{col, desc, lit, rank, row_number}
import org.apache.spark.sql.types._

import scala.util.{Failure, Success, Try}

object YoutubeAnalytics extends LazyLogging {

  def createTopCategory(videoInformation: DataFrame, categoryEvent: Int, top: Int): DataFrame = {
    Try {
      //Selecciona la columna por la cual se hará la window function
      val orderColumn = categoryEvent match {
        case 1 => VIEWS
        case 2 => LIKES
        case 3 => DISLIKES
        case _ =>
          throw new NullPointerException("Category Event does not exist. Only exist categories: \n1. Views\n2. Likes\n 3. Dislikes")
      }

      //Traemos los videos con la fecha más reciente para evitar duplicados
      //Así evitamos que si un video sale más de una vez, también lo haga en el top
      val windowForRecent = Window.partitionBy(VIDEO_ID).orderBy(desc(TRENDING_DATE))
      val recentVideos = videoInformation.select(col("*"), row_number.over(windowForRecent).as("row")).
        filter(col("row") === 1)


      //Sacamos el Top sin videos duplicados
      val windowForTop = Window.partitionBy(YEAR).orderBy(desc(orderColumn))
      recentVideos.select(col("*"), lit(categoryEvent).as(CATEGORY_EVENT), rank().over(windowForTop).as("top")).
        filter(col("top") <= top).select(col(COUNTRY),
        col(COUNTRY_NAME),
        col(VIDEO_ID),
        col(YEAR),
        col(TRENDING_DATE).cast(DateType),
        col(TITLE),
        col(CHANNEL_TITLE),
        col(CATEGORY_ID).cast(IntegerType),
        col(TAGS),
        col(VIEWS).cast(LongType),
        col(LIKES).cast(LongType),
        col(DISLIKES).cast(LongType),
        col(CATEGORY_EVENT).cast(StringType))
    } match {
      case Success(value) => value
      case Failure(exception: NullPointerException) => logger.error("Null dataframe detected in createTopCategory"); throw exception
      case Failure(exception: AnalysisException) => logger.error("Column not found in createTopCategory"); throw exception
      case Failure(other) => throw other
    }
  }

  def joinChannelsTop(channels: DataFrame, videoInformation: DataFrame): DataFrame = {
    Try {
      //Hacemos el join final y casteamos los campos para la salida final
      videoInformation.join(channels, videoInformation(VIDEO_ID) === channels(VIDEO_ID)).select(
        channels(COUNTRY),
        channels(COUNTRY_NAME),
        videoInformation(VIDEO_ID),
        videoInformation(YEAR),
        videoInformation(TRENDING_DATE).cast(DateType),
        channels(TITLE),
        col(CHANNEL_TITLE),
        videoInformation(CATEGORY_ID).cast(IntegerType),
        channels(TAGS),
        videoInformation(VIEWS).cast(LongType),
        videoInformation(LIKES).cast(LongType),
        videoInformation(DISLIKES).cast(LongType))
    } match {
      case Success(value) => value
      case Failure(exception: NullPointerException) => logger.error("Null dataframe detected in joinChannelsTop"); throw exception
      case Failure(exception: AnalysisException) => logger.error("Column not found in joinChannelsTop"); throw exception
      case Failure(other) => throw other
    }
  }
}
