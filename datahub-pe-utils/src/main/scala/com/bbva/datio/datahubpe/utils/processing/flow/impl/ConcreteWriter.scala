package com.bbva.datio.datahubpe.utils.processing.flow.impl

import com.bbva.datio.datahubpe.utils.processing.conf.SchemaConfigValidator
import com.bbva.datio.datahubpe.utils.processing.data.DataWriter
import com.bbva.datio.datahubpe.utils.processing.flow.{EnvironmentKeyConfigReader, Writer}
import com.datio.kirby.CheckFlow
import com.datio.kirby.config.OutputFactory
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class ConcreteWriter(spark: SparkSession, config: Config) extends Writer[DataWriter] with OutputFactory with CheckFlow {
  override def write(dataWriter: DataWriter): Unit = {
    val outputKeyConfigReader = new OutputKeyConfigReader(config)
    val schemaConfigValidator = new SchemaConfigValidator(config)
    outputKeyConfigReader
      .getKeys()
      .foreach(key => {
        val itemWriter           = dataWriter.getItemWriter(key)
        val acceptableOutputConf = schemaConfigValidator.replaceLabels(outputKeyConfigReader.path + "." + key)

        writeDataFrame(itemWriter.df, readOutput(acceptableOutputConf))
      })
  }
}
