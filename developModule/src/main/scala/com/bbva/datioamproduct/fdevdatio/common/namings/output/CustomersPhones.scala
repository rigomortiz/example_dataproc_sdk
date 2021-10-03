package com.bbva.datioamproduct.fdevdatio.common.namings.output

import com.bbva.datioamproduct.fdevdatio.common.namings.Field
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Phones.{Brand, DiscountAmount, PriceProduct, Prime, StockNumber, Taxes}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{current_date, months_between, when, dense_rank, lit, floor}
import com.bbva.datioamproduct.fdevdatio.common.StaticVals._
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Customers.BirthDate
import org.apache.spark.sql.expressions.{Window, WindowSpec}

object CustomersPhones {

  case object CustomerVip extends Field {
    override val name = "customer_vip"

    def apply(): Column = {
      when(Prime.column === YES && PriceProduct.column >= 7500, YES)
        .otherwise(NO)
        .alias(name)
    }
  }

  case object DiscountExtra extends Field {
    override val name = "discount_extra"

    def apply(): Column = {
      when(
        Prime.column === YES &&
          StockNumber.column < THIRTY_FIVE &&
          !Brand.column.isin("XOLO", "Siemens", "Panasonic", "BlackBerry"),
        PriceProduct.column * TEN_PERCENT
      ).otherwise(ZERO_DOUBLE).alias(name)
    }
  }

  case object FinalPrice extends Field {
    override val name = "final_price"

    def apply(): Column = {
      (PriceProduct.column + Taxes.column - DiscountAmount.column - DiscountExtra.column).alias(name)
    }
  }

  case object Age extends Field {
    override val name = "age"

    def apply(): Column = {
      floor((months_between(current_date(), BirthDate.column) / TWELVE)).alias(name)
    }
  }

  case object BrandsTop extends Field {
    override val name: String = "brands_top"

    def apply(): Column = {
      val w: WindowSpec = Window.partitionBy(Brand.column).orderBy(FinalPrice.column.desc)
      dense_rank().over(w).alias(name)
    }
  }

  case object JwkDate extends Field {
    override val name: String = "jwk_date"

    def apply(jwkDate: String): Column = {
      lit(jwkDate).alias(name)
    }
  }

}
