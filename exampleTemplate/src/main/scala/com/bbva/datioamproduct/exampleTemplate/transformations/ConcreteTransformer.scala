package com.bbva.datioamproduct.exampleTemplate.transformations

import com.bbva.datioamproduct.utils.data.{DataReader, DataWriter}
import com.bbva.datioamproduct.utils.flow.Transformer
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class ConcreteTransformer(config: Config) extends Transformer[DataReader, DataWriter] {

  override def transform(dataReader: DataReader): DataWriter = {
    val dataWriter = new DataWriter()

    val managerFiltered = getManagerFiltered(dataReader)
    dataReader.add("managerFiltered", managerFiltered)

    val structureFiltered = getStructureFiltered(dataReader)
    dataReader.add("structureFiltered", structureFiltered)

    val managerUpdateBankingServiceByRank = changeExecutiveType(dataReader)
    dataReader.add("managerUpdateBankingServiceByRank", managerUpdateBankingServiceByRank)

    val joinManagerStructure = getJoinManagerStructure(dataReader)
    dataWriter.add("beygmanager1", joinManagerStructure,true)

    dataWriter
  }

  def getManagerFiltered(dataReader: DataReader): DataFrame = {

    val informationDate = config.getString("appJob.inputs.manager.information_date")

    val filterBankingService: Column = col("executive_banking_service_type").isin("PE", "MA", "DO", "ML")
    val filterManagerEmployee: Column = !col("manager_employee_name").like("%WORKSITE%") or
      !col("manager_employee_name").like("%PYME%")
    val filterInformationDate: Column = col("information_date") === informationDate

    val manager: DataFrame = dataReader.get("manager")
    val managerFiltered = manager.where(filterBankingService and filterManagerEmployee and filterInformationDate)
    managerFiltered

  }

  def getStructureFiltered(dataReader: DataReader): DataFrame = {
    val cutoffDate = config.getString("appJob.inputs.structure.cutoff_date")

    val filterCutoffDate: Column = col("cutoff_date") === cutoffDate
    val filterBankingclsfn: Column = col("banking_clsfn_branch_type").isin("E", "G")
    val filterBankingType: Column = col("banking_type").isin("9047", "9105")
    val filterBranchStatusType: Column = col("branch_status_type") === "VIGENTE"

    val structure: DataFrame = dataReader.get("structure")
    val structureFiltered = structure.where(filterCutoffDate and filterBankingclsfn and filterBankingType and filterBranchStatusType)
    structureFiltered
  }

  def changeExecutiveType(dataReader: DataReader): DataFrame = {

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

  def getJoinManagerStructure(dataReader: DataReader): DataFrame = {
    val joinManagerStructure = dataReader.get("managerUpdateBankingServiceByRank")
      .join(dataReader.get("structureFiltered"), Seq("branch_id"), "left_semi")
      .select("branch_id", "manager_id",
        "manager_employee_name", "manager_status_type",
        "manager_register_time_date", "executive_banking_service_type",
        "manager_entry_date", "manager_register_branch_id",
        "information_date", "cutoff_date")

    joinManagerStructure

  }
}
