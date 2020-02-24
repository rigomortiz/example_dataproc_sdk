package com.bbva.datio.datahubpe.utils.processing.conf

import com.datio.kirby.constants.ConfigConstants.SCHEMA_CONF_KEY
import com.typesafe.config.{Config, ConfigValueFactory}

class SchemaConfigValidator {
  private val Path          = "path"
  private val SchemaPathKey = s"$SCHEMA_CONF_KEY.$Path"
  def replaceLabels(config: Config, environment: String): Config = {
    if (config.hasPath(SchemaPathKey)) {
      val schemaPath         = config.getString(SchemaPathKey)
      val replacedSchemaPath = ArtifactoryPathFactory(environment).build(schemaPath)
      config.withValue(SchemaPathKey, ConfigValueFactory.fromAnyRef(replacedSchemaPath))
    } else {
      config
    }
  }
}
