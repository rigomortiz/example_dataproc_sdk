package com.bbva.datio.datahubpe.exampleTemplate.transformations

import com.bbva.datio.datahubpe.utils.processing.data.DataReader
import com.bbva.datio.datahubpe.utils.processing.flow.Transformer
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, when}

class ManagerUpdateBankingServiceByRankTransformer (config: Config)  extends Transformer[DataReader, DataFrame]{
 override def transform(dataReader: DataReader): DataFrame = {
   val managerUpdateBankingService = dataReader.get("managerFiltered")
     .withColumn("executive_banking_service_type",
                 when(col("executive_banking_service_type") === "ML", "PE")
                   .otherwise(col("executive_banking_service_type")))

   val windowsGroupByBranch = row_number().over(Window.partitionBy("branch_id").orderBy(col("manager_id")))
   val managerUpdateBankingServiceByBranch = managerUpdateBankingService.withColumn("Row_number", windowsGroupByBranch)
   val managerUpdateBankingServiceByRank = managerUpdateBankingServiceByBranch
     .withColumn("executive_banking_service_type",
                 when(col("Row_number") === 1, "DO")
                   .otherwise(col("executive_banking_service_type")))

   managerUpdateBankingServiceByRank

 }
}
