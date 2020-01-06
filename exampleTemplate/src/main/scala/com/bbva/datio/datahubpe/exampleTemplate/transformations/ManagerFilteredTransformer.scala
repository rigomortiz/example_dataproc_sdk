package com.bbva.datio.datahubpe.exampleTemplate.transformations

import com.bbva.datio.datahubpe.utils.processing.data.DataReader
import com.bbva.datio.datahubpe.utils.processing.flow.Transformer
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

class ManagerFilteredTransformer(config: Config)  extends Transformer[DataReader, DataFrame]{
 override def transform(dataReader: DataReader): DataFrame = {

   val informationDate = config.getString("appJob.inputs.manager.information_date")

   val filterBankingService: Column = col("executive_banking_service_type").isin("PE", "MA", "DO", "ML")
   val filterManagerEmployee: Column = !col("manager_employee_name").like("%WORKSITE%") or
                                       !col("manager_employee_name").like("%PYME%")
   val filterInformationDate: Column = col("information_date") === informationDate

   val manager: DataFrame = dataReader.get("manager")
   val managerFiltered = manager.where(filterBankingService and filterManagerEmployee and filterInformationDate)
   managerFiltered

 }
}
