package com.bbva.datio.datahubpe.utils.processing.flow.impl

import com.datio.kirby.constants.ConfigConstants.SCHEMA_CONF_KEY
import com.datio.kirby.schema.validation.SchemaReader
import com.typesafe.config.Config
import org.apache.spark.sql.types.StructType

class ConcreteSchemaReader extends SchemaReader {
  def read(config: Config): StructType = {
    if (config.hasPath(SCHEMA_CONF_KEY)) {
      val schemaConfig = config.getConfig(SCHEMA_CONF_KEY)
      readSchema(schemaConfig, includeMetadata = true).getOrElse(new StructType())
    } else {
      new StructType()
    }
  }
}
