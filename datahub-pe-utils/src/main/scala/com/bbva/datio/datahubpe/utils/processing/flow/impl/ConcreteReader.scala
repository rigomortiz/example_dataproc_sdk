package com.bbva.datio.datahubpe.utils.processing.flow.impl

import com.bbva.datio.datahubpe.utils.processing.conf.SchemaConfigValidator
import com.bbva.datio.datahubpe.utils.processing.data.DataReader
import com.bbva.datio.datahubpe.utils.processing.flow.Reader
import com.datio.dataproc.kirby.api.Input
import com.datio.dataproc.kirby.core.config.configurable.KirbyConfigurableFactory
import com.typesafe.config.Config

class ConcreteReader(config: Config) extends Reader[DataReader] {
  private val dataReader: DataReader = new DataReader()
  private val inputFactory = new KirbyConfigurableFactory[Input]()

  override def read(): DataReader = {
    val inputConfigReader = new InputKeyConfigReader(config)
    val keys = inputConfigReader.getKeys()
    val schemaConfigValidator = new SchemaConfigValidator(config)
    keys.foreach(key => {
      val configInput = schemaConfigValidator.replaceLabels(inputConfigReader.path + "." + key)
      val df = inputFactory.getImplementation(configInput).read()
      dataReader.add(key, df)
    })

    dataReader
  }
}
