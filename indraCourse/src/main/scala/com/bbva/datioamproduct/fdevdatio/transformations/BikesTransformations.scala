package com.bbva.datioamproduct.fdevdatio.transformations

import com.bbva.datioamproduct.fdevdatio.constants.GeneralConstants._
import com.bbva.datioamproduct.fdevdatio.constants.fields.input.Bikes._
import org.apache.spark.sql.{Dataset, Row}

object BikesTransformations {

  implicit class BikesDs(ds: Dataset[Row]) {

    /**
     * Filtrar la tabla t_fdev_bikes:
     * * Por talla de bicicleta (size) diferente de S.
     */
    def filterSizeColumn(): Dataset[Row] = {
      ds.filter(Size.column =!= SizeS)
    }

  }

}
