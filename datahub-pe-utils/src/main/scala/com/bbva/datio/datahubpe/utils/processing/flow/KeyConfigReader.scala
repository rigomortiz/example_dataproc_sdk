package com.bbva.datio.datahubpe.utils.processing.flow

import com.typesafe.config.Config

import scala.collection.JavaConversions.{asScalaSet, mapAsScalaMap}

trait KeyConfigReader {
  val path: String = ""
  val config: Config

  final def getKeys(): List[String] =
    config
      .getObject(path)
      .keySet()
      .map { key =>
        s"$key"
      }
      .toList

  final def getKeyRoot(): String = config.root().keys.toList.filter(x => x.endsWith("Job")).apply(0)
}
