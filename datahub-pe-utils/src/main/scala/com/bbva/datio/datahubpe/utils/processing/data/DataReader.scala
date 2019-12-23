package com.bbva.datio.datahubpe.utils.processing.data

import org.apache.spark.sql.DataFrame

class DataReader {
  private val input = collection.mutable.Map[String, DataFrame]()

  def add(name: String, df: DataFrame): Unit = {
    input += (name -> df)
  }

  def get(key: String): DataFrame = {
    if (!contains(key)) {
      throw new RuntimeException("repeat dataframe")
    }
    input.get(key).get
  }

  def size(): Int = {

    input.size
  }

  def contains(key: String): Boolean = input.contains(key)


}
