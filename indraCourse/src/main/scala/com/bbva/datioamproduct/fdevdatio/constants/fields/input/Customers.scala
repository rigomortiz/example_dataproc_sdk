package com.bbva.datioamproduct.fdevdatio.constants.fields.input

import com.bbva.datioamproduct.fdevdatio.constants.fields.Field

object Customers {

  case object Name extends Field {
    override val name: String = "name"
  }

  case object Country extends Field {
    override val name: String = "country"
  }

  case object BikeId extends Field {
    override val name: String = "bike_id"
  }

  case object PurchaseYear extends Field {
    override val name: String = "purchase_year"
  }

  case object PurchaseCity extends Field {
    override val name: String = "purchase_city"
  }

  case object PurchaseContinent extends Field {
    override val name: String = "purchase_continent"
  }

  case object PurchaseOnline extends Field {
    override val name: String = "purchase_online"
  }

}
