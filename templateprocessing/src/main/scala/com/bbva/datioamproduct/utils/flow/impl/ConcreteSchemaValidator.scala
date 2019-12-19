package com.bbva.datioamproduct.utils.flow.impl

import cats.data.Validated.{Invalid, Valid}
import com.bbva.datioamproduct.utils.data.DataWriter
import com.bbva.datioamproduct.utils.flow.Validator
import com.datio.kirby.api.exceptions.KirbyException
import com.datio.kirby.errors.SCHEMA_VALIDATION_ERROR
import com.datio.kirby.schema.validation.{SchemaReader, SchemaValidator}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

class ConcreteSchemaValidator(spark: SparkSession, config: Config)
  extends Validator[DataWriter] with SchemaReader with SchemaValidator {

  override def validate(dataWriter: DataWriter): DataWriter = {

    val ouputKeyConfigReader = new OuputKeyConfigReader(config)
    ouputKeyConfigReader.getKeys().foreach(key => {
    if (dataWriter.valideSchema(key)){
      val schemaConf = config.getConfig(ouputKeyConfigReader.path + "." + key + "." + "schema")
      val schema = readSchema(schemaConf, includeMetadata = true).getOrElse(new StructType())
      validateDF(dataWriter.get(key), schema) match {
        case Invalid(error) =>
          throw new KirbyException(SCHEMA_VALIDATION_ERROR,
            error.map(_.toString).toList.mkString(", "))
        case Valid(dataFrame) =>
          logger.info("The validation has been finished sucessfully.")
          dataFrame
      }
    }
    }

    )
    dataWriter
  }


}
