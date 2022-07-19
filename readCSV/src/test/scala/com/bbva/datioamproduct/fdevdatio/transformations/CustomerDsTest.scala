package com.bbva.datioamproduct.fdevdatio.transformations

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants.{ABR, BIKES_CONFIG, CUSTOMER_BIKES_CONFIG, CUSTOMER_CONFIG, MESSAGE, MESSAGE_COL}
import com.bbva.datioamproduct.fdevdatio.common.StaticValues.TOKYO
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Bikes.Size
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Customer.{Country, PurchaseCity}
import com.bbva.datioamproduct.fdevdatio.common.namings.output.CustomerBikes.{IsHybridCustomer, IsInPlaceCustomer,
  IsOnlineCustomer, NBikes, TotalInPlace, TotalOnline, TotalRefund, TotalSpent}
import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils
import org.apache.spark.sql.DataFrame
import com.bbva.datioamproduct.fdevdatio.transformations.Transformations.{BikesDf, CustomerBikesDf, CustomerDf}
import org.apache.spark.sql.functions.col


class CustomerDsTest extends ContextProvider with IOUtils {
  "filterSize method" should "return a DS with values 'M' or 'L' in column 'size'" in {
    val dataFrame: DataFrame = read(config.getConfig(BIKES_CONFIG))
    val output = dataFrame.filterSize()
    output.filter(Size.column === "M" && Size.column === "L").count shouldBe 0
  }

  "addMessage method" should "return a DS with column 'message'" in {
    val dataFrame: DataFrame = read(config.getConfig(CUSTOMER_CONFIG))
    val output = dataFrame.addMessage(config.getString(MESSAGE))
    output.columns contains MESSAGE_COL shouldBe true
  }

  "lowerCountry method" should "return a DS with column abr with country in lowercase" in {
    val dataFrame: DataFrame = read(config.getConfig(CUSTOMER_CONFIG))
    val output = dataFrame.lowerCountry()
    output.columns contains Country.name shouldBe true
    output.filter(Country.column.rlike("^[A-Z]*$")).count shouldBe 0
  }

  "filterYearAndCity method" should "return a DS without values 'Tokio' into purchase_city" in {
    val dataFrame: DataFrame = read(config.getConfig(CUSTOMER_CONFIG))
    val output = dataFrame.filterYearAndCity()
    output.filter(PurchaseCity.column === TOKYO).count shouldBe 0
  }

  "joinBikes method" should "return a DS with columns from customerDs and bikesDs" in {
    val dataFrame: DataFrame = read(config.getConfig(BIKES_CONFIG))
    val output = dataFrame.joinBikes(dataFrame)

    output.columns.length shouldBe 17
    output.columns should contain allOf ("bike_id", "brand", "type", "brakes", "size", "material", "price",
      "bike_name", "wheel_size")
  }

  "replacePurchaseCity method" should "return a DS with column 'purchase_city' in uppercase" in {
    val dataFrame: DataFrame = read(config.getConfig(CUSTOMER_CONFIG))
    val output = dataFrame.replacePurchaseCity
    output.filter(PurchaseCity.column.rlike("^[a-z]*$")).count shouldBe 0
  }

  "addColumns method" should "return a DS with columns" in {
    val dataFrame: DataFrame = read(config.getConfig(CUSTOMER_CONFIG))
    val outputBikesDf: DataFrame = read(config.getConfig(BIKES_CONFIG)).filterSize()
    val output = dataFrame
      .joinBikes(outputBikesDf)
      .addColumns(NBikes(),
      TotalSpent(),
      TotalOnline(),
      TotalInPlace(),
      IsInPlaceCustomer(),
      IsOnlineCustomer(),
      IsHybridCustomer(),
      TotalRefund())
    output.columns should contain allOf (NBikes.name, TotalSpent.name, TotalOnline.name, TotalInPlace.name,
    IsInPlaceCustomer.name, IsOnlineCustomer.name, IsHybridCustomer.name, TotalRefund.name)
  }


}
