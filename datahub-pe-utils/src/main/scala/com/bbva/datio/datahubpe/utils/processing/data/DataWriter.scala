package com.bbva.datio.datahubpe.utils.processing.data

import org.apache.spark.sql.DataFrame

class DataWriter  {
  private val output = collection.mutable.Map[String, ItemWriter]()

  def add(key: String, df: DataFrame, valideSchema: Boolean = true): Unit = {
    if (contains(key)) {
      throw new RuntimeException("la Key ya esta siendo utilizado por otro DataFrame")
    }
    output += (key -> ItemWriter(df, valideSchema))

  }

  def getItemWriter(key: String): ItemWriter= {
    if (!contains(key)) {
      throw new RuntimeException("La Key no existe dentro del DataWriter")
    }
    output.get(key).get
  }


  /** Identifica por medio de la key si un elemento se encuentra en el DataReader
    *
    * @param keyes la llave de identificación del dataframe
    * @return contiene un elemento*/
  def contains(key: String): Boolean = output.contains(key)
}
