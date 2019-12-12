package com.bbva.datioamproduct.utils.flow.impl

import com.bbva.datioamproduct.utils.data.DataWriter
import com.bbva.datioamproduct.utils.flow.Writer
import com.datio.kirby.CheckFlow
import com.datio.kirby.config.OutputFactory
import com.typesafe.config.Config

import org.apache.spark.sql.SparkSession

class ConcreteWriter(spark: SparkSession, config: Config) extends Writer[DataWriter] with OutputFactory with CheckFlow {
  override def write(dataWriter: DataWriter): Unit = {

    val ouputKeyConfigReader = new OuputKeyConfigReader(config)
    ouputKeyConfigReader.getKeys().foreach(key => {
      val outputConf = config.getConfig(ouputKeyConfigReader.path + "." + key)
      dataWriter.get(key).show()
      writeDataFrame(dataWriter.get(key), readOutput(outputConf))

    })
  }
}
