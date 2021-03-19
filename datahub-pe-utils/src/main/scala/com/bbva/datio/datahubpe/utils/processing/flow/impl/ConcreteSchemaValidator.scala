package com.bbva.datio.datahubpe.utils.processing.flow.impl

import cats.data.Validated.{Invalid, Valid}
import com.bbva.datio.datahubpe.utils.processing.conf.SchemaConfigValidator
import com.bbva.datio.datahubpe.utils.processing.data.DataWriter
import com.bbva.datio.datahubpe.utils.processing.flow.{Validator}
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.SCHEMA_VALIDATION_ERROR
import com.datio.kirby.schema.validation.{SchemaValidator}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class ConcreteSchemaValidator(spark: SparkSession, config: Config) extends Validator[DataWriter] with SchemaValidator {
  override def validate(dataWriter: DataWriter): DataWriter = {
    val outputKeyConfigReader = new OutputKeyConfigReader(config)
    val schemaConfigValidator = new SchemaConfigValidator(config)
    val schemaReader          = new ConcreteSchemaReader()
    outputKeyConfigReader
      .getKeys()
      .foreach(key => {
        val itemWriter = dataWriter.getItemWriter(key)
        if (itemWriter.schemaValidation) {
          val acceptableOutputConfig = schemaConfigValidator.replaceLabels(outputKeyConfigReader.path + "." + key)
          val schema                 = schemaReader.read(acceptableOutputConfig)
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
