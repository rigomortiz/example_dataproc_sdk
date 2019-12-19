package com.bbva.datioamproduct.utils.com.flow

import com.typesafe.config.Config
import scala.collection.JavaConversions._


trait KeyConfigReader {
  val path: String =""
  val config: Config
  final def getKeys():List[String]= config.getObject(path).keySet().map{ key =>s"$key"}.toList


  final def getKeyRoot():String = config.root().keys.toList.filter(x => x.endsWith("Job")).apply(0)

}