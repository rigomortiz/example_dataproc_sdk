package com.bbva.datio.datahubpe.utils.processing.flow.impl

import com.bbva.datio.datahubpe.utils.processing.data.DataWriter
import com.bbva.datio.datahubpe.utils.processing.flow.Writer
import com.datio.kirby.CheckFlow
import com.datio.kirby.config.OutputFactory
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

class ConcreteWriter(spark: SparkSession, config: Config) extends Writer[DataWriter] with OutputFactory with CheckFlow {
  override def write(dataWriter: DataWriter): Unit = {

    val ouputKeyConfigReader = new OuputKeyConfigReader(config)
    ouputKeyConfigReader.getKeys().foreach(key => {
      val itemWriter = dataWriter.getItemWriter(key)
      val outputConf = config.getConfig(ouputKeyConfigReader.path + "." + key)
      writeDataFrame(itemWriter.df, readOutput(outputConf))
    })
  }
}
