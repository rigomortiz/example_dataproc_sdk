package com.bbva.datio.datahubpe.utils.processing.flow.impl

import cats.data.Validated.{Invalid, Valid}
import com.bbva.datio.datahubpe.utils.processing.conf.SchemaConfigValidator
import com.bbva.datio.datahubpe.utils.processing.data.DataWriter
import com.bbva.datio.datahubpe.utils.processing.flow.{EnvironmentKeyConfigReader, Validator}
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.SCHEMA_VALIDATION_ERROR
import com.datio.kirby.constants.ConfigConstants.SCHEMA_CONF_KEY
import com.datio.kirby.schema.validation.{SchemaReader, SchemaValidator}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class ConcreteSchemaValidator(spark: SparkSession, config: Config)
    extends Validator[DataWriter]
    with SchemaReader
    with SchemaValidator {
  override def validate(dataWriter: DataWriter): DataWriter = {
    val outputKeyConfigReader = new OutputKeyConfigReader(config)
    val schemaConfigValidator = new SchemaConfigValidator(config)
    outputKeyConfigReader
      .getKeys()
      .foreach(key => {
        val itemWriter = dataWriter.getItemWriter(key)
        if (itemWriter.schemaValidation) {
          val acceptableOutputConf = schemaConfigValidator.replaceLabels(outputKeyConfigReader.path + "." + key)
          val schemaConf           = acceptableOutputConf.getConfig(SCHEMA_CONF_KEY)
          val schema               = readSchema(schemaConf, includeMetadata = true).getOrElse(new StructType())
          validateDF(itemWriter.df, schema) match {
            case Invalid(error) =>
              throw new KirbyException(SCHEMA_VALIDATION_ERROR, error.map(_.toString).toList.mkString(", "))
            case Valid(dataFrame) => dataFrame
          }
        }
      })
    dataWriter
  }
}
