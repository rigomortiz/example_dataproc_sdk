package com.bbva.datioamproduct.datahubutils

import com.datio.spark.test.ContextProvider
import org.apache.spark.sql.AnalysisException
import org.scalatest.{Matchers, WordSpec}


class ExampleTest extends WordSpec with Matchers with ContextProvider {

  def getResourcesPath(resource: String): String = getClass.getClassLoader.getResource(resource).getFile

  "A Example" when {

    "reading a dataset" should {

      "throw an exception if the dataset does not exist" in {
        val invalidPath = "nonexistent-file.txt"

        assertThrows[AnalysisException] {
          Example.countWords(spark, invalidPath)
        }
      }

      "return the word counter" in {
        val inputPath = getResourcesPath("data/wordcount-input.txt")

        val df = Example.countWords(spark, inputPath)
        val counts = df.collect.map(r => (r.getAs[String]("word"), r.getAs[Int]("count"))).toMap
        counts("for") shouldBe 2
        counts("And") shouldBe 3
      }
    }
  }

}
