package com.bbva.datio.datahubpe.exampleTemplate.transformations

import com.bbva.datio.datahubpe.utils.processing.data.{DataReader, DataWriter}
import com.bbva.datio.datahubpe.utils.processing.flow.Transformer
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class ConcreteTransformer(config: Config) extends Transformer[DataReader, DataWriter] {

  override def transform(dataReader: DataReader): DataWriter = {
    val dataWriter = new DataWriter()
    dataReader.add("managerFiltered", new ManagerFilteredTransformer(config).transform(dataReader))
    dataReader.add("structureFiltered", new StructureFilteredTransformer (config).transform(dataReader))
    dataReader.add("managerUpdateBankingServiceByRank", new ManagerUpdateBankingServiceByRankTransformer(config)
                                                               .transform(dataReader))
    dataWriter.add("beygmanager1", new JoinManagerStructureTransformer(config).transform(dataReader),true)
    dataWriter
  }





}
