package com.bbva.datioamproduct.utils.flow.impl



import com.bbva.datioamproduct.utils.data.{DataReader, DataWriter}
import com.bbva.datioamproduct.utils.flow.Transformer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import com.typesafe.config.Config

class ConcreteTransformer(config:Config) extends Transformer[DataReader,DataWriter]{
  override def transform(dataReader: DataReader): DataWriter = {
    val dataWriter = new DataWriter()
   val informationDate = config.getString("CptInvoicesJob.inputs.manager.information_date")

    val filterBankingService = col("executive_banking_service_type").isin("PE", "MA", "DO", "ML")
    val filterManagerEmployee = !col("manager_employee_name").like("%WORKSITE%") or
                                !col("manager_employee_name").like("%PYME%")

    val filterInformationDate = col("information_date") === informationDate

    val manager: DataFrame = dataReader.get("manager")
    val managerFiltered = manager.where(filterBankingService and filterManagerEmployee )
    dataWriter.add("outputFinal", managerFiltered)
    dataWriter
  }



}
