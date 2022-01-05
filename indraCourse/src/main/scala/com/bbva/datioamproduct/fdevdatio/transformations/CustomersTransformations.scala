package com.bbva.datioamproduct.fdevdatio.transformations

import com.bbva.datioamproduct.fdevdatio.constants.fields.input.Customers._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.{current_date, year}
import com.bbva.datioamproduct.fdevdatio.constants.GeneralConstants._

object CustomersTransformations {

  implicit class CustomersDs(ds: Dataset[Row]) {


    /**
     * Filtrar la tabla t_fdev_customer:
     * * Conservar solo los registros que tengan una fecha de compra (purchase_year)
     * mayor a “<current_date> - 10” y que hayan sido realizadas (purchase_city) en una ciudad diferente a Tokyo.
     * NOTA: current_date no debe ser un valor en duro.
     */
    def filterPurchaseYear(): Dataset[Row] = {
      ds
        .filter(
          PurchaseYear.column > year(current_date()) - TenNumber &&
            PurchaseCity.column =!= TokyoString
        )
    }

  }

}
