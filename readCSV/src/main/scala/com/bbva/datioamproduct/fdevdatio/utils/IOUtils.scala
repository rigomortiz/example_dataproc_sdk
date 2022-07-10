package com.bbva.datioamproduct.fdevdatio.utils

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants._
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.io.output.DatioDataFrameWriter
import com.datio.dataproc.sdk.schema.DatioSchema
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}

import java.net.URI

trait IOUtils {
  lazy val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()

  def read(inputConfig: Config): DataFrame = {
    val path: String = inputConfig.getStringList(PATHS).get(0)
    inputConfig.getString(TYPE) match {
      case "parquet" => datioSparkSession.read().parquet(path)
      case "csv" => {
        val schemaPath:String = inputConfig.getString(SCHEMA)
        val schema: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
        val delimiter: String = inputConfig.getString(DELIMITER)
        datioSparkSession.read()
          .option(HEADERS, true.toString)
          .option(DELIMITER, delimiter)
          .datioSchema(schema)
          .csv(path)
      }
      case _@inputType => throw new Exception(s"Formato de archivo no soportado: $inputType")
    }
  }

  def write(outputConfig: Config, df: DataFrame): Unit = {
    val mode: SaveMode = outputConfig.getString(MODE) match {
      case OVERWRITE => SaveMode.Overwrite
      case APPEND => SaveMode.Append
      case _@saveMode => throw new Exception(s"Modo de escritura no soportado: $saveMode")
    }
    val path: String = outputConfig.getString(PATH)
    val schemaPath: String = outputConfig.getString(SCHEMA)
    val schema: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
    val partitions: Array[String] = outputConfig.getList(PARTITIONS).toArray.map(_.toString)

    val write: DatioDataFrameWriter = datioSparkSession
      .write()
      .mode(mode)
      .datioSchema(schema)
      .partitionBy(partitions: _*)

    outputConfig.getString(TYPE) match {
      case CSV => write.csv(df, path)
      case AVRO => write.avro(df, path)
      case PARQUET => write.parquet(df, path)
      case _@outputType => throw new Exception(s"Formato no soportado: $outputType")
    }

  }
}
