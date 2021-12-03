package com.bbva.datioamproduct.fdevdatio.transformations

import com.bbva.datioamproduct.fdevdatio.constants.StaticVals.JoinTypes
import com.bbva.datioamproduct.fdevdatio.constants.namings.input.Customers.{CreditCardNumber, CustomerId, DeliveryId}
import com.bbva.datioamproduct.fdevdatio.constants.namings.input.Phones.CountryCode
import com.bbva.datioamproduct.fdevdatio.constants.namings.output.CustomersPhones.{BrandsTop, CustomerVip, ExtraDiscount, FinalPrice}
import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.transformations.Transformations.{CustomersPhonesTransformer, CustomersTransformer, PhonesTransformer}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

class TransformationsTest extends ContextProvider {
  "filterPhones method" should
    "return a DF without values CH, IT, CZ y DK in column country_code" in {


    val inputDF: PhonesTransformer = jobConfig.fDevPhones.read()
    val outputDF = inputDF.filterPhones()

    outputDF
      .filter(CountryCode.column.isin("CH", "IT", "CZ", "DK")).count() shouldBe 0
  }

  "filterCustomers method" should
    "return a DF with shortest lengths than 17 in column credit_card_number" in {

    val inputDF: CustomersTransformer = jobConfig.fDevCustomers.read()
    val outputDF = inputDF.filterCustomers()

    outputDF
      .filter(!CreditCardNumber.filter).count() shouldBe 0
  }

  "addColumn method" should
    "add a Column to the DataFrame" in {

    val phonesDF: PhonesTransformer = jobConfig.fDevPhones.read()
    val customersDF: CustomersTransformer = jobConfig.fDevCustomers.read()

    val inputDF = phonesDF.filterPhones().join(
      customersDF.filterCustomers(),
      Seq(CustomerId.name, DeliveryId.name),
      JoinTypes.INNER
    )

    val testColumn: Column = lit("example_value").alias("test_column")
    val outputDF = inputDF.addColumn(testColumn)

    outputDF.columns should contain("test_column")
  }

  "filterBrandsTop method" should
    "add one Column brands_top and return a DF with values less than 51" in {

    val phonesDF: PhonesTransformer = jobConfig.fDevPhones.read()
    val customersDF: CustomersTransformer = jobConfig.fDevCustomers.read()

    val inputDF = phonesDF.filterPhones().join(
      customersDF.filterCustomers(),
      Seq(CustomerId.name, DeliveryId.name),
      JoinTypes.INNER
    ).addColumn(CustomerVip())
      .addColumn(ExtraDiscount())
      .addColumn(FinalPrice()) // Necesitamos final_price para probar la regla que genera brands_top

    val outputDF = inputDF.filterBrandsTop()

    inputDF.columns should not contain ("brands_top")
    outputDF.columns should contain("brands_top")
    outputDF.filter(BrandsTop.column > 50).count() should be(0)

  }
}
