package com.bbva.datio.datahubpe.utils.processing.flow.impl

import com.bbva.datio.datahubpe.utils.processing.flow.KeyConfigReader
import com.typesafe.config.Config

class OuputKeyConfigReader(override val config: Config) extends KeyConfigReader {
  override val path: String = getKeyRoot() + ".outputs"
}
