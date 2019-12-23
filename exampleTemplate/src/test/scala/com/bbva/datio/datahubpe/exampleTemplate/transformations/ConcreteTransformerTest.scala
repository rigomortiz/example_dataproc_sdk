package com.bbva.datio.datahubpe.exampleTemplate.transformations

import com.bbva.datio.datahubpe.utils.processing.data.DataReader
import com.bbva.datio.datahubpe.utils.processing.flow.impl.ConcreteReader
import com.datio.spark.test.ContextProvider
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

class ConcreteTransformerTest extends FlatSpec with Matchers with ContextProvider {
  val config = ConfigFactory.defaultApplication()
  val dataReader = new DataReader()
  val concreteTransformer = new ConcreteTransformer(config)

  it should "return los dataframe filtrados según las condiciones" in {
    val inputCorrect =
      """
        |appJob{
        |inputs{
        |manager{
        |paths = ["src/test/resources/data/input/t_mdco_tcom_manager"]
        |type = parquet
        |information_date = "2018-10-23"
        |}
        |structure{
        |paths = ["src/test/resources/data/input/t_mdco_branch_structure"]
        |type = parquet
        |cutoff_date = "2019-09-01"
        |}}}
          """.stripMargin
    val configFile = ConfigFactory.parseString(inputCorrect)

    val concreteReader = new ConcreteReader(spark,configFile).read()
    concreteTransformer.getManagerFiltered(concreteReader)
    concreteTransformer.getStructureFiltered(concreteReader)
    succeed
  }

  it should "return valores actualizados según executive_banking_service_type" in {

    val data = Seq(
      Row("PE", "0001", "007004"),
      Row("MA", "0001", "007005"),
      Row("DO", "0002", "007006"),
      Row("ML", "0002", "007007")
    )

    val schema = List(
      StructField("executive_banking_service_type", StringType, true),
      StructField("branch_id", StringType, true),
      StructField("manager_id", StringType, true)
    )
    val dataManager = spark.createDataFrame(spark.sparkContext.parallelize(data), StructType(schema))
    dataReader.add("managerFiltered", dataManager)

    val df = concreteTransformer.changeExecutiveType(dataReader)
    val datos = df.collect.map(r => (r.getAs[String]("branch_id"),
      r.getAs[String]("executive_banking_service_type")
    )).toMap
    datos("0001") shouldBe "MA"
    datos("0002") shouldBe "PE"
  }

  it should "return valores solo de la tabla managers y no tenga conexión con strcture" in {

    val schemaManager = List(
      StructField("branch_id", StringType, true),
      StructField("manager_id", StringType, true),
      StructField("manager_employee_name", StringType, true),
      StructField("manager_status_type", StringType, true),
      StructField("manager_register_time_date", StringType, true),
      StructField("executive_banking_service_type", StringType, true),
      StructField("manager_entry_date", StringType, true),
      StructField("manager_register_branch_id", StringType, true),
      StructField("information_date", StringType, true),
      StructField("cutoff_date", StringType, true)
    )


    val Manager = Seq(
      Row("0001", "007004", "juan datio", "s", "2018-08-08", "MA", "2018-07-07", "007004", "2019-01-01", "2018-08-23"),
      Row("0002", "007005", "juan datio", "s", "2018-08-08", "PE", "2018-07-07", "007005", "2019-01-01", "2018-08-23"),
      Row("0003", "007006", "juan datio", "s", "2018-08-08", "PE", "2018-07-07", "007006", "2019-01-01", "2018-08-23"),
      Row("0004", "007007", "juan datio", "s", "2018-08-08", "DO", "2018-07-07", "007007", "2019-01-01", "2018-08-23")
    )
    val dataManager = spark.createDataFrame(spark.sparkContext.parallelize(Manager), StructType(schemaManager))
    dataReader.add("managerUpdateBankingServiceByRank", dataManager)

    val schemaStructure = List(StructField("branch_id", StringType, true))
    val Structure = Seq(Row("0001"))

    val dataStructure = spark.createDataFrame(spark.sparkContext.parallelize(Structure), StructType(schemaStructure))
    dataReader.add("structureFiltered", dataStructure)
    val df = concreteTransformer.getJoinManagerStructure(dataReader)

    val datos = df.collect.map(r => (r.getAs[String]("branch_id"),
      r.getAs[String]("manager_id")
    )).toMap
    datos("0001") shouldBe "007004"

    assertResult(1)(df.count)

  }

}
