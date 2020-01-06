package com.bbva.datio.datahubpe.utils.processing.flow.impl

import cats.data.Validated.{Invalid, Valid}
import com.bbva.datio.datahubpe.utils.processing.data.DataWriter
import com.bbva.datio.datahubpe.utils.processing.flow.Validator
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.SCHEMA_VALIDATION_ERROR
import com.datio.kirby.schema.validation.{SchemaReader, SchemaValidator}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class ConcreteSchemaValidator(spark: SparkSession,
                              config: Config) extends Validator[DataWriter] with SchemaReader with SchemaValidator {

  override def validate(dataWriter: DataWriter): DataWriter = {

    val ouputKeyConfigReader = new OuputKeyConfigReader(config)
    ouputKeyConfigReader.getKeys().foreach(key => {
      val itemWriter = dataWriter.getItemWriter(key)
      if (itemWriter.schemaValidation) {
        val schemaConf = config.getConfig(s"$ouputKeyConfigReader.path.$key.schema")
        val schema = readSchema(schemaConf, includeMetadata = true).getOrElse(new StructType())
        validateDF(itemWriter.df, schema) match {
          case Invalid(error) => throw new KirbyException(SCHEMA_VALIDATION_ERROR,
                                                          error.map(_.toString).toList.mkString(", "))
          case Valid(dataFrame) => dataFrame
        }
      }
    })
    dataWriter
  }


}
