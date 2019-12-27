package com.bbva.datio.datahubpe.utils.processing.data

import org.apache.spark.sql.DataFrame

class DataWriter  {
  private val output = collection.mutable.Map[String, ItemWriter]()

  def add(name: String, df: DataFrame, valideSchema:Boolean = true): Unit = {

        output += (name ->  ItemWriter(df,valideSchema))

  }

  def getItemWriter(name: String): ItemWriter= {
    if (!contains(name)) {
      throw new RuntimeException("no found dataframe")
    }
    output.get(name).get
  }


  /** Identifica por medio de la key si un elemento se encuentra en el DataReader
    *
    * @param keyes la llave de identificación del dataframe
    * @return contiene un elemento*/
  def contains(key: String): Boolean = output.contains(key)
}
