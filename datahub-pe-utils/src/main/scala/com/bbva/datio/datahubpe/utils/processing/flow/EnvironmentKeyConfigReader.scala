package com.bbva.datio.datahubpe.utils.processing.flow

import com.bbva.datio.datahubpe.utils.processing.conf.ArtifactoryPathFactory.EnvironmentType
import com.typesafe.config.Config

import scala.collection.JavaConversions.{asScalaSet, mapAsScalaMap}
import scala.util.Try
class EnvironmentKeyConfigReader(val config: Config) {
  private final def getKeyRoot: String = config.root().keys.toList.filter(x => x.endsWith("Job")).apply(0)

  final def getEnvironmentKey: String = {
    Try(config.getString(getKeyRoot + "params.environment")).getOrElse(EnvironmentType.Local)
  }
}
