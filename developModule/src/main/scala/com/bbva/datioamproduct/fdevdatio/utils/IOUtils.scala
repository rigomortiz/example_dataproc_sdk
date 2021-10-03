package com.bbva.datioamproduct.fdevdatio.utils

import java.net.URI

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants._
import com.datio.dataproc.sdk.datiosparksession.DatioSparkSession
import com.datio.dataproc.sdk.io.output.DatioDataFrameWriter
import com.datio.dataproc.sdk.schema.DatioSchema
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SaveMode}

trait IOUtils {
  val datioSparkSession: DatioSparkSession = DatioSparkSession.getOrCreate()

  def read(inputConfig: Config): DataFrame = {
    val path: String = inputConfig.getString(PATH)

    inputConfig.getString(TYPE) match {
      case "parquet" => datioSparkSession.read().parquet(path)
      case _@inputType => throw new Exception(s"Formato de archivo no soportado: $inputType")
    }

  }

  def write(outputConfig: Config, df: DataFrame): Unit = {

    val mode: SaveMode = outputConfig.getString(MODE) match {
      case "overwrite" => SaveMode.Overwrite
      case _@saveMode => throw new Exception(s"Modo de escritura no soportado: $saveMode")
    }
    val path: String = outputConfig.getString(PATH)
    val schemaPath: String = outputConfig.getString(SCHEMA)
    val schema: DatioSchema = DatioSchema.getBuilder.fromURI(URI.create(schemaPath)).build()
    val partitions: Array[String] = outputConfig.getStringList(PARTITIONS).toArray.map(_.toString)

    val writer: DatioDataFrameWriter = datioSparkSession
      .write()
      .mode(mode)
      .datioSchema(schema)
      .partitionBy(partitions: _*)

    outputConfig.getString(TYPE) match {
      case "parquet" => writer.parquet(df, path)
      case "csv" => writer.csv(df, path)
      case "avro" => writer.avro(df, path)
      case _@outputType => throw new Exception(s"Formato de escritura no soportado: $outputType")
    }
  }
}
