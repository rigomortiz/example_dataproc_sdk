package com.bbva.datioamproduct.fdevdatio.common.namings.output

import com.bbva.datioamproduct.fdevdatio.common.namings.Field
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Bikes.Price
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Customer.{Name, PurchaseOnline, PurchaseYear}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions.{count, sum, udf, when}

object CustomerBikes {
  private val windowByName: WindowSpec = Window.partitionBy(Name.column)

  case object NBikes extends Field {
    override lazy val name: String = "n_bikes"

    def apply(): Column = {
      // sin orderBy cuenta todos los registros
      windowByName.orderBy(PurchaseYear.column)
      count(Name.column) over windowByName alias name
    }
  }

  case object TotalSpent extends Field {
    override lazy val name: String = "total_spent"
    def apply(): Column = {
      /**
       *        row_number, rank, dense_rank
       * A  19  1          1     1
       * A  15  2          2     2
       * A  15  3          2     2
       * A  14  4          4     3
       * A  14  5          4     3
       * A  14  6          4     3
       * A  10  7          7     4
       *
       * collect_list
       */
      sum(Price.column) over windowByName alias name
    }
  }

  case object TotalOnline extends Field {
    override lazy val name: String = "total_online"

    def apply(): Column = {
      sum(
        when(PurchaseOnline.column === 1, 1) otherwise 0
      ) over windowByName alias name
    }
  }

  case object TotalInPlace extends Field {
    override lazy val name: String = "total_in_place"

    def apply(): Column = {
      sum(
        when(PurchaseOnline.column =!= 1, 0) otherwise 1
      ) over windowByName alias name
    }
  }

  case object IsOnlineCustomer extends Field {
    override val name: String = "in_line_customer"

    def apply(): Column = {
      when(TotalOnline.column > TotalInPlace.column, true) otherwise false alias name
    }
  }

  case object IsInPlaceCustomer extends Field {
    override val name: String = "is_in_place_customer"

    def apply(): Column = {
      when(TotalOnline.column < TotalInPlace.column, true) otherwise false alias name
    }
  }

  case object IsHybridCustomer extends Field {
    override val name: String = "is_hybrid_customer"

    def apply(): Column = {
      when(TotalOnline.column === TotalInPlace.column, true) otherwise false alias name
    }
  }

  case object TotalRefund extends Field {
    override val name: String = "total_refund"

    def apply(): Column = {
      when(IsOnlineCustomer.column, TotalSpent.column * 0.10)
        . when(IsInPlaceCustomer.column, TotalSpent.column * 0.05)
        .otherwise(TotalSpent.column * 0.08)
        .alias(name)
    }
  }

  case object PurchaseCity extends Field {
    override val name: String = "purchase_city"

    def apply(): Column = {
      val f: UserDefinedFunction = udf {
        purchaseCity: String => {
          purchaseCity.toUpperCase()
        }
      }
      f(PurchaseCity.column).alias(name)
    }
  }
}
