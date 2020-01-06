package com.bbva.datio.datahubpe.exampleTemplate.transformations

import com.bbva.datio.datahubpe.utils.processing.data.DataReader
import com.bbva.datio.datahubpe.utils.processing.flow.Transformer
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame

class JoinManagerStructureTransformer (config: Config)  extends Transformer[DataReader, DataFrame]{
 override def transform(dataReader: DataReader): DataFrame = {
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
