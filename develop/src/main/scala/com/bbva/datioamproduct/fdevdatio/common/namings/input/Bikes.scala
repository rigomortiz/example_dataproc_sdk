package com.bbva.datioamproduct.fdevdatio.common.namings.input

import com.bbva.datioamproduct.fdevdatio.common.namings.Field

object Bikes {
  case object BikeId extends Field {
    override val name: String = "bike_id"
  }
  case object Brand extends Field {
    override val name: String = "brand"
  }
  case object Type extends Field {
    override val name: String = "type"
  }
  case object BikeName extends Field {
    override val name: String = "bike_name"
  }
  case object WheelSize extends Field {
    override val name: String = "wheel_size"
  }
  case object Brakes extends Field {
    override val name: String = "brakes"
  }
  case object Size extends Field {
    override val name: String = "size"
  }
  case object Material extends Field {
    override val name: String = "material"
  }
  case object Price extends Field {
    override val name: String = "price"
  }
}
