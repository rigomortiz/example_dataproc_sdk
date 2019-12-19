package com.bbva.datioamproduct.utils.flow.impl



import com.bbva.datioamproduct.utils.com.flow.KeyConfigReader
import com.typesafe.config.Config
import scala.collection.JavaConversions._

class InputKeyConfigReader(override val config: Config) extends KeyConfigReader {
   override val path: String = getKeyRoot() + ".inputs"




}
