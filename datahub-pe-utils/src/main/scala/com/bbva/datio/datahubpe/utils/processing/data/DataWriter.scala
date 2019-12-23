package com.bbva.datio.datahubpe.utils.processing.data

import org.apache.spark.sql.DataFrame

class DataWriter  {
  private val output = collection.mutable.Map[String, ItemWriter]()

  def add(name: String, df: DataFrame, validationSchema:Boolean = true): Unit = {

        output += (name ->  ItemWriter(df,validationSchema))

  }

  def get(name: String): DataFrame = {
    if (!output.contains(name)) {
      throw new RuntimeException("no found dataframe")
    }
    output.get(name).get.df
  }

  def valideSchema(name: String) : Boolean ={
    if (!output.contains(name)) {
      throw new RuntimeException("no found dataframe")
    }

    output.get(name).get.schemaValidation
  }
}
