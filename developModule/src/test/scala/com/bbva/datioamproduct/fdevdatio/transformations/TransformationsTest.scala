package com.bbva.datioamproduct.fdevdatio.transformations

import com.bbva.datioamproduct.fdevdatio.common.ConfigConstants._
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Customers.CreditCardNumber
import com.bbva.datioamproduct.fdevdatio.common.namings.input.Phones.CountryCode
import com.bbva.datioamproduct.fdevdatio.testUtils.ContextProvider
import com.bbva.datioamproduct.fdevdatio.transformations.Transformations.{CustomersTransformer, PhonesTransformer}
import com.bbva.datioamproduct.fdevdatio.utils.IOUtils

class TransformationsTest extends ContextProvider with IOUtils {
  "filterPhones method" should
    "return a DF without values CH, IT, CZ y DK in column country_code" in {


    val inputDF: PhonesTransformer = read(config.getConfig(PHONES_CONFIG))
    val ouptutDF = inputDF.filterPhones()

    ouptutDF
      .filter(CountryCode.column.isin("CH", "IT", "CZ", "DK")).count() shouldBe 0
  }

  "filterCustomers method" should
    "return a DF with shortest lengths than 17 in column credit_card_number" in {

    val inputDF: CustomersTransformer = read(config.getConfig(CUSTOMERS_CONFIG))
    val ouptutDF = inputDF.filterCustomers()

    ouptutDF
      .filter(!CreditCardNumber.filter).count() shouldBe 0
  }
}
