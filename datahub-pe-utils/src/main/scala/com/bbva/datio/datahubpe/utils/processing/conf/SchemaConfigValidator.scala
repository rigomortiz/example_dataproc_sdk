package com.bbva.datio.datahubpe.utils.processing.conf

import com.bbva.datio.datahubpe.utils.processing.flow.EnvironmentKeyConfigReader
import com.datio.kirby.constants.ConfigConstants.SCHEMA_CONF_KEY
import com.typesafe.config.{Config, ConfigValueFactory}

class SchemaConfigValidator(config: Config) {
  private val Path                    = "path"
  private val SchemaPathKey           = s"$SCHEMA_CONF_KEY.$Path"
  private val environmentConfigReader = new EnvironmentKeyConfigReader(config)
  private val environmentType         = environmentConfigReader.getEnvironmentKey
  def replaceLabels(path: String): Config = {
    val keyConfig = config.getConfig(path)
    if (keyConfig.hasPath(SchemaPathKey)) {
      val schemaPath         = keyConfig.getString(SchemaPathKey)
      val replacedSchemaPath = ArtifactoryPathFactory(environmentType).build(schemaPath)
      keyConfig.withValue(SchemaPathKey, ConfigValueFactory.fromAnyRef(replacedSchemaPath))
    } else {
      keyConfig
    }
  }
}
