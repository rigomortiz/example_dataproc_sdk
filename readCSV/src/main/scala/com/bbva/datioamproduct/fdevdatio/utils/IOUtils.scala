package com.bbva.datioamproduct.fdevdatio.utils

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants.{BIKES_CONFIG, DELIMITER, HEADERS, PATHS, SCHEMA, TYPE}
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.schema.DatioSchema
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

import java.net.URI

trait IOUtils {
  lazy val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()
  def read(inputConfig: Config): DataFrame = {
    val path: String = inputConfig.getStringList(PATHS).get(0)
    inputConfig.getString(TYPE) match {
      case "parquet" => datioSparkSession.read().parquet(path)
      case "csv" => {
        val schemaPath:String = inputConfig.getString(SCHEMA)
        val schemaBikes: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
        val delimiter: String = inputConfig.getString(DELIMITER)
        datioSparkSession.read()
          .option(HEADERS, true.toString)
          .option(DELIMITER, delimiter)
          .datioSchema(schemaBikes)
          .csv(path)
      }
    }
  }
}
