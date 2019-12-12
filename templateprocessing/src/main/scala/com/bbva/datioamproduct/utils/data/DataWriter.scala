package com.bbva.datioamproduct.utils.data

import org.apache.spark.sql.DataFrame

class DataWriter  {
  private val output = collection.mutable.Map[String, DataFrame]()

  def add(name: String, df: DataFrame): Unit = {
    output += (name -> df)
  }

  def get(name: String): DataFrame = {
    if (!output.contains(name)) {
      throw new RuntimeException("no found dataframe")
    }
    output.get(name).get
  }
}
