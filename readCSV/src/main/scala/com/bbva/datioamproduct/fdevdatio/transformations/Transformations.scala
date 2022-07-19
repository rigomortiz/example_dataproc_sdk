package com.bbva.datioamproduct.fdevdatio.transformations

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants.{ABR, MESSAGE_COL}
import com.bbva.datioamproduct.fdevdatio.common.StaticValues.{AMERICA, TOKYO}
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Bikes.{BikeId, Size}
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Customer.{Country, PurchaseContinent, PurchaseOnline, PurchaseYear}
import com.bbva.datioamproduct.fdevdatio.common.namings.output.CustomerBikes.PurchaseCity
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, lower, row_number}

import java.util.{Calendar, Date}


object Transformations {
  implicit class BikesDf(dataFrame: DataFrame) {
    def filterSize(): DataFrame = {
      dataFrame.filter(Size.column === "M" || Size.column === "L")
    }
  }

  implicit class CustomerDf(df: DataFrame) {
    /**
     * Add text into column message
     * @param message String message
     * @return DataFrame
     */
    def addMessage(message: String): DataFrame = {
      /*
      +: element concat array
      :+ array concat element
      ++ concat
       */
      df.select(df.columns.map(s => col(s)) :+ lit(message).alias(MESSAGE_COL) :_*)
    }

    /**
     * Lower column country
     * @return DataFrame
     */
    def lowerCountry(): DataFrame = {
      df.select(df.columns.map {
        case name: String if name == Country.name => lower(Country.column).alias(Country.name)
        case _@name => col(name)
      } :_*)
    }

    /**
     * Filtrar la tabla t_fdev_customer, conservar solo los registrosque tengan una fecha de compra(purchase_year)
     * mayor a current_day - 10 years y que hayan sido realizadas(purchase_city) en una ciudad diferente a tokio
     * Nota current_date no debe de ser un valor en duro
     * @return
     */
    def filterYearAndCity(): DataFrame = {
      df
        .filter(PurchaseYear.column > Calendar.getInstance().get(Calendar.YEAR) - 10)
        .filter(PurchaseCity.column =!= TOKYO)
    }

    /**
     * Relation
     * 1. col("x") >= col("y") en SQL ON
     * 2. Seq("Key_column") en SQL USING
     * @param bikesDf
     * @return
     */
    def joinBikes(bikesDf: DataFrame): DataFrame = {
      // df.join(bikesDf, df(BikeId.name) === bikesDf(BikeId.name), "inner")
      // df.join(bikesDf, Seq(BikeId.name), "inner")
      df.join(bikesDf, BikeId.name)
    }
  }

  implicit class CustomerBikesDf(df: DataFrame) {
    def addColumns(columns: Column*): DataFrame = {
      columns.toList match {
        case Nil => df
        case h::t =>  df.select(df.columns.map(col) :+ h :_*).addColumns(t:_*)
      }
    }

    def replacePurchaseCity: DataFrame = {
      df.select(
        df.columns.map {
          case PurchaseCity.name => PurchaseCity()
          case _@name => col(name)
        }: _*
      )
    }
  }

}
