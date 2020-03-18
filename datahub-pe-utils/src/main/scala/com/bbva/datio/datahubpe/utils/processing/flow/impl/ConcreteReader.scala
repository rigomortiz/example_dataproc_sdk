package com.bbva.datio.datahubpe.utils.processing.flow.impl

import com.bbva.datio.datahubpe.utils.processing.conf.SchemaConfigValidator
import com.bbva.datio.datahubpe.utils.processing.data.DataReader
import com.bbva.datio.datahubpe.utils.processing.flow.{EnvironmentKeyConfigReader, Reader}
import com.datio.kirby.CheckFlow
import com.datio.kirby.config.InputFactory
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class ConcreteReader(spark: SparkSession, config: Config) extends Reader[DataReader] with InputFactory with CheckFlow {
  private val dataReader: DataReader = new DataReader()

  override def read(): DataReader = {
    val inputConfigReader     = new InputKeyConfigReader(config)
    val keys                  = inputConfigReader.getKeys()
    val schemaConfigValidator = new SchemaConfigValidator(config)
    keys.foreach(key => {
      val configInput = schemaConfigValidator.replaceLabels(inputConfigReader.path + "." + key)
      val df          = readDataFrame(readInput(configInput))(spark)
      dataReader.add(key, df)
    })

    dataReader
  }
}
