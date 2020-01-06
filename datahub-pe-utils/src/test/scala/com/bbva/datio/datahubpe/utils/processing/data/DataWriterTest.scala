package com.bbva.datio.datahubpe.utils.processing.data

import com.datio.spark.test.ContextProvider
import org.apache.spark.sql.DataFrame
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataWriterTest  extends FlatSpec with ContextProvider with Matchers with
  BeforeAndAfter {
  var df: DataFrame = _
  var dataWriter: DataWriter = _
  before {
           dataWriter = new DataWriter()
           val columns = Seq("language", "users_count")
           val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
           val rdd = spark.sparkContext.parallelize(data)
           df = spark.createDataFrame(rdd).toDF(columns: _*)
         }

  "Data Writer" should "contain one element " in {
    dataWriter.add("df1", df)
    assert(dataWriter.contains("df1"))
  }

  it should "produce an exception when key dataframe exists in DataReader " in {
    dataWriter.add("df1", df)

    an[RuntimeException] should be thrownBy dataWriter.add("df1", df)


  }
  it should "get a element" in {
    dataWriter.add("df1", df)
    val dfExpected = dataWriter.getItemWriter("df1")
    assert(dfExpected.df.count() == 3)

  }
  it should "produce an exception when key dataframe not exists in DataReader " in {
    dataWriter.add("df1", df)

    an[RuntimeException] should be thrownBy dataWriter.getItemWriter("df2")


  }
}