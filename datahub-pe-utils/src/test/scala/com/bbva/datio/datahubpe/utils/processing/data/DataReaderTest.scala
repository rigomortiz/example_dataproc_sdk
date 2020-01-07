package com.bbva.datio.datahubpe.utils.processing.data


import com.datio.spark.test.ContextProvider
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class DataReaderTest extends FlatSpec with ContextProvider with Matchers with
  BeforeAndAfter {
  var df: DataFrame = _
  var dataReader: DataReader = _
  before {
           dataReader = new DataReader()
           val columns = Seq("language", "users_count")
           val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
           val rdd = spark.sparkContext.parallelize(data)
           df = spark.createDataFrame(rdd).toDF(columns: _*)
         }

  "Data Reader " should "add one element " in {
    dataReader.add("df1", df)
    assert(dataReader.size() == 1)
  }

  it should "contain one element " in {
    dataReader.add("df1", df)
    assert(dataReader.contains("df1"))
  }

  it should "produce an exception when key dataframe exists in DataReader " in {
    dataReader.add("df1", df)

    an[RuntimeException] should be thrownBy dataReader.add("df1", df)


  }
  it should "get a element" in {
    dataReader.add("df1", df)
    val dfExpected = dataReader.get("df1")
    assert(dfExpected.count() == 3)

  }
  it should "produce an exception when key dataframe not exists in DataReader " in {
    dataReader.add("df1", df)

    an[RuntimeException] should be thrownBy dataReader.get("df2")


  }
}
