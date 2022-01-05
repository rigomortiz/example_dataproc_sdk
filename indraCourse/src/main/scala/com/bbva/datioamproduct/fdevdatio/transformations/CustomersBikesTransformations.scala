package com.bbva.datioamproduct.fdevdatio.transformations

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, Dataset, Row}

object CustomersBikesTransformations {

  implicit class CustomersBikesDs(ds: Dataset[Row]) {

    def addColumns(columns: Column*): Dataset[Row] = {
      columns.toList match {
        case Nil => ds
        case h :: t => {
          ds
            .select(
              ds.columns.map(col) :+ h: _*
            )
            .addColumns(t: _*)
        }
      }
    }

  }

}
