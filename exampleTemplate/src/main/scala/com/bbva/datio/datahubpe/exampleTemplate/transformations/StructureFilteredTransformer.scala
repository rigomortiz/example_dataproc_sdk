package com.bbva.datio.datahubpe.exampleTemplate.transformations

import com.bbva.datio.datahubpe.utils.processing.data.DataReader
import com.bbva.datio.datahubpe.utils.processing.flow.Transformer
import com.typesafe.config.Config
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

class StructureFilteredTransformer (config: Config)  extends Transformer[DataReader, DataFrame]{
 override def transform(dataReader: DataReader): DataFrame = {

   val cutoffDate = config.getString("appJob.inputs.structure.cutoff_date")

   val filterCutoffDate: Column = col("cutoff_date") === cutoffDate
   val filterBankingclsfn: Column = col("banking_clsfn_branch_type").isin("E", "G")
   val filterBankingType: Column = col("banking_type").isin("9047", "9105")
   val filterBranchStatusType: Column = col("branch_status_type") === "VIGENTE"

   val structure: DataFrame = dataReader.get("structure")
   val structureFiltered = structure.where(filterCutoffDate and filterBankingclsfn and filterBankingType and filterBranchStatusType)
   structureFiltered
 }
}
