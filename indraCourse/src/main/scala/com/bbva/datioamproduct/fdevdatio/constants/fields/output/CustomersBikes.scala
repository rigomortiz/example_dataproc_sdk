package com.bbva.datioamproduct.fdevdatio.constants.fields.output

import com.bbva.datioamproduct.fdevdatio.constants.GeneralConstants._
import com.bbva.datioamproduct.fdevdatio.constants.fields.Field
import com.bbva.datioamproduct.fdevdatio.constants.fields.input.Bikes.Price
import com.bbva.datioamproduct.fdevdatio.constants.fields.input.Customers.{Name, PurchaseOnline}
import com.bbva.datioamproduct.fdevdatio.constants.fields.output.CustomersBikes.TotalOnline.name
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{count, sum, when}
import org.apache.spark.sql.types.{BooleanType, DataType, DecimalType, DoubleType, IntegerType}

object CustomersBikes {

  private val windowByCustomerName: WindowSpec = Window.partitionBy(Name.name)

  /**
   * El Sr.Wick necesita conocer la cantidad de bicicletas (n_bikes) por cada cliente (name).
   * * NOTA: n_bikes es un campo calculado.
   */
  case object NBikes extends Field {
    override val name: String = "n_bikes"
    val dataType: DataType = IntegerType

    def apply(): Column = {
      count(All).over(windowByCustomerName).cast(dataType).alias(name)
    }
  }

  /**
   * El Sr.Wick necesita conocer la cantidad de bicicletas (n_bikes) y el monto total de las
   * compras (total_spent) por cada cliente (name).
   * * NOTA: n_bikes y total_spent son campos calculados.
   */
  case object TotalSpent extends Field {
    override val name: String = "total_spent"
    val dataType: DataType = IntegerType

    def apply(): Column = {
      sum(Price.column).over(windowByCustomerName).cast(dataType).alias(name)
    }
  }

  /**
   * El cálculo de total_online y total_inplace debes realizarlo a través del campo
   * purchase_online, el cual se encuentra en true si fue una compra en linea y false en caso de
   * ser una compra en sitio.
   * EJEMPLO: Para Fernando en el campo purchase_online hay 3 true y 4 false, entonces:
   * total_online = 3
   * total_inplace = 4
   */
  case object TotalOnline extends Field {
    override val name: String = "total_online"
    val dataType: DataType = IntegerType

    def apply(): Column = {
      sum(
        when(PurchaseOnline.column === true, OneNumber).otherwise(ZeroNumber)
      )
        .over(windowByCustomerName)
        .cast(dataType)
        .alias(name)
    }
  }

  /**
   * El cálculo de total_online y total_inplace debes realizarlo a través del campo
   * purchase_online, el cual se encuentra en true si fue una compra en linea y false en caso de
   * ser una compra en sitio.
   * EJEMPLO: Para Fernando en el campo purchase_online hay 3 true y 4 false, entonces:
   * total_online = 3
   * total_inplace = 4
   */
  case object TotalInplace extends Field {
    override val name: String = "total_inplace"
    val dataType: DataType = IntegerType

    def apply(): Column = {
      sum(
        when(PurchaseOnline.column, ZeroNumber).otherwise(OneNumber)
      )
        .over(windowByCustomerName)
        .cast(dataType)
        .alias(name)
    }
  }

  /**
   * ● si total_online > total_inplace => is_online_customer es true y las otras dos false.
   */
  case object IsOnlineCustomer extends Field {
    override val name: String = "is_online_customer"
    val dataType: DataType = BooleanType

    def apply(): Column = {
      (TotalOnline.column > TotalInplace.column).alias(name)
    }
  }

  /**
   * ● si total_online < total_inplace => is_inplace_customer es true y las otras dos false.
   */
  case object IsInplaceCustomer extends Field {
    override val name: String = "is_inplace_customer"
    val dataType: DataType = BooleanType

    def apply(): Column = {
      (TotalOnline.column < TotalInplace.column).alias(name)
    }
  }

  /**
   * ● si total_online == total_inplace => is_hybrid_customer es true y las otras dos false.
   */
  case object IsHybridCustomer extends Field {
    override val name: String = "is_hybrid_customer"
    val dataType: DataType = BooleanType

    def apply(): Column = {
      (TotalOnline.column === TotalInplace.column).alias(name)
    }
  }

  /**
   *
   * Se requiere saber el monto total a devolver a cada cliente con las siguientes reglas.
   * ● A los usuarios que son online se les deberá regresa un 10% de sus compras totales
   * ● A los usuarios que son físicos se les deberá regresa un 5% de sus compras totales
   * ● A los usuarios que son Hybrid se les deberá regresa un 8% de sus compras totales
   * NOTA: Guardar el monto a devolver en un campo adicional llamado “total_refund”
   */
  case object TotalRefund extends Field {
    override val name: String = "total_refund"
    val dataType: DataType = DecimalType(TwelveNumber, TwoNumber)

    def apply(): Column = {
      when(IsOnlineCustomer.column, TotalSpent.column * TenPercent)
        .when(IsInplaceCustomer.column, TotalSpent.column * FivePercent)
        .when(IsHybridCustomer.column, TotalSpent.column * EightPercent)
        .otherwise(ZeroNumber)
        .cast(dataType)
        .alias(name)
    }
  }

}