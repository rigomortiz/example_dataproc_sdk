package com.bbva.datio.datahubpe.utils.processing.data

import org.apache.spark.sql.DataFrame

class DataReader {
  private val input = collection.mutable.Map[String, DataFrame]()

  /**
    * Agrega un dataframe a una colección
    *
    * @param key es la llave de identificación del dataframe
    * @param df  Dataframe
    */
  def add(key: String, df: DataFrame): Unit = {
    if (contains(key)) {
      throw new RuntimeException("la Key ya esta siendo utilizado por otro DataFrame")
    }

    input += (key -> df)
  }

  /**
    * Obtiene un dataframe por su key
    *
    * @param key es la llave de identificación del dataframe
    * @return un Dataframe
    */
  def get(key: String): DataFrame = {
    if (!contains(key)) {
      throw new RuntimeException("La Key no existe dentro del DataReader")
    }
    input.get(key).get
  }

  /**
    * Devuelve el número de elementos
    *
    * @return número de elementos
    */
  def size(): Int = input.size

  /**
    * Identifica por medio de la key si un elemento se encuentra en el DataReader
    *
    * @param key la llave de identificación del dataframe
    * @return contiene un elemento
    */
  def contains(key: String): Boolean = input.contains(key)
}
