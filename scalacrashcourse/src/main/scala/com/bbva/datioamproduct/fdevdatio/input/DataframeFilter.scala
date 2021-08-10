package com.bbva.datioamproduct.fdevdatio.input

import com.bbva.datioamproduct.fdevdatio.common.Columns._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, LongType}

import scala.util.{Failure, Success, Try}

object DataframeFilter extends LazyLogging {

  def getChannelsTable(dataframe: DataFrame, country: String): DataFrame = {
    Try {
      // Filtramos y seleccionamos SOLO la información que necesitaremos
      // Las funciones When pueden ser implementados con UDFs
      dataframe.filter(col(COUNTRY) === country).
        select(col(VIDEO_ID), col(CHANNEL_TITLE), col(TITLE), col(TAGS), col(COUNTRY),
          when(col(COUNTRY) === "CA", "Canadá").otherwise(
            when(col(COUNTRY) === "DE", "Alemania").otherwise(
              when(col(COUNTRY) === "FR", "Francia").otherwise(
                when(col(COUNTRY) === "GB", "Gran Bretaña").otherwise(
                  when(col(COUNTRY) === "US", "Estados Unidos").otherwise(
                    when(col(COUNTRY) === "JP", "Japón").otherwise(
                      when(col(COUNTRY) === "MX", "México").otherwise(
                        when(col(COUNTRY) === "RU", "Rusia").otherwise(
                          when(col(COUNTRY) === "IN", "India").otherwise("unknown"))))))))).as(COUNTRY_NAME)).
        dropDuplicates(Seq(VIDEO_ID))
    } match {
      case Success(value) => value
      case Failure(exception: NullPointerException) => logger.error("Null dataframe detected in getChannelsTable"); throw exception
      case Failure(exception: AnalysisException) => logger.error("Column not found in getChannelsTable"); throw exception
      case Failure(other) => throw other
    }
  }

  def getVideoInfo(dataFrame: DataFrame, year: Int): DataFrame = {
    // Sacamos los últimos 2 números del año para filtrar la información
    val yearToStringYY = year.toString.substring(2,4)

    Try {
      // Seleccionamos y filtramos SOLO por los campos que necesitamos, además de castear los campos porque los datos número
      // vienen en formato String, esto nos puede causar un desorden en la transformación
      dataFrame.filter(col(TRENDING_DATE).startsWith(yearToStringYY)).
        select(col(VIDEO_ID), col(CATEGORY_ID).cast(IntegerType), col(VIEWS).cast(LongType),
          col(LIKES).cast(LongType), col(DISLIKES).cast(LongType), lit(year).as(YEAR),
          date_format(to_date(col(TRENDING_DATE), "yy.dd.MM"), "yyyy-MM-dd").cast(DateType).
            as(TRENDING_DATE))
    } match {
      case Success(value) => value
      case Failure(exception: NullPointerException) => logger.error("Null dataframe detected in getVideoInfo"); throw exception
      case Failure(exception: AnalysisException) => logger.error("Column not found in getVideoInfo"); throw exception
      case Failure(other) => throw other
    }
  }
}
