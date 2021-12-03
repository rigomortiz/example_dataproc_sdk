package com.bbva.datioamproduct.fdevdatio.constants.namings.output

import com.bbva.datioamproduct.fdevdatio.constants.namings.Field
import com.bbva.datioamproduct.fdevdatio.constants.namings.input.Phones.{Brand, DiscountAmount, PriceProduct, Prime, StockNumber, Taxes}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{current_date, dense_rank, floor, lit, months_between, when}
import com.bbva.datioamproduct.fdevdatio.constants.StaticVals._
import com.bbva.datioamproduct.fdevdatio.constants.namings.input.Customers.BirthDate
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.types.{DateType, DecimalType, IntegerType}

object CustomersPhones {

  case object CustomerVip extends Field {
    override val name = "customer_vip"

    def apply(): Column = {
      when(Prime.column === YES && PriceProduct.column >= 7500, YES)
        .otherwise(NO)
        .alias(name)
    }
  }

  case object ExtraDiscount extends Field {
    override val name = "extra_discount"

    def apply(): Column = {
      when(
        Prime.column === YES &&
          StockNumber.column < THIRTY_FIVE &&
          !Brand.column.isin("XOLO", "Siemens", "Panasonic", "BlackBerry"),
        PriceProduct.column * TEN_PERCENT
      ).otherwise(ZERO_DOUBLE).cast(DecimalType(9, 2)).alias(name)
    }
  }

  case object FinalPrice extends Field {
    override val name = "final_price"

    def apply(): Column = {
      (PriceProduct.column + Taxes.column - DiscountAmount.column - ExtraDiscount.column)
        .cast(DecimalType(9, 2)).alias(name)
    }
  }

  case object Age extends Field {
    override val name = "age"

    def apply(): Column = {
      floor((months_between(current_date(), BirthDate.column) / TWELVE)).cast(IntegerType).alias(name)
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
      lit(jwkDate).cast(DateType).alias(name)
    }
  }

}
