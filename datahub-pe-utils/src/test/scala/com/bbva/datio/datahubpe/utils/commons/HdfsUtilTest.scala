package com.bbva.datio.datahubpe.utils.commons

import java.io.{FileNotFoundException, IOException}

import com.bbva.datio.datahubpe.utils.BaseSpec
import org.scalatest.BeforeAndAfter

class HdfsUtilTest extends BaseSpec with BeforeAndAfter {
  behavior of "Hdfs Util"
  it should " return a list of values from a path" in {
    val path               = "src/test/resources/data/input/t_mdco_branch_structure"
    val expectedPartitions = Seq("2018-09-01", "2018-10-01")
    val actualPartitions   = HdfsUtil.listPartitionValues(spark, path, "cutoff_date")
    actualPartitions should be(expectedPartitions)
  }

  it should " return a last partition" in {
    val path               = "src/test/resources/data/input/t_mdco_branch_structure"
    val expectedPartitions = "2018-10-01"
    val actualPartitions   = HdfsUtil.getLastPartition(spark, path, "cutoff_date", "")
    actualPartitions should be(expectedPartitions)
  }
  it should " return a last partition when date is 2018-09-30" in {
    val path               = "src/test/resources/data/input/t_mdco_branch_structure"
    val expectedPartitions = "2018-09-01"
    val actualPartitions   = HdfsUtil.getLastPartition(spark, path, "cutoff_date", "2018-09-30")
    actualPartitions should be(expectedPartitions)
  }
  it should "produce an exception when path no exists" in {
    val path = "src/test/resources/data/input/not_valid"

    val error = intercept[FileNotFoundException] {
      HdfsUtil.listPartitionValues(spark, path, "cutoff_date")
    }
    error.getMessage should be("File src/test/resources/data/input/not_valid does not exist")
  }

  it should "produce an exception when directory is empty" in {
    val path = "src/test/resources/data/input/empty"

    val error = intercept[FileNotFoundException] {
      HdfsUtil.listPartitionValues(spark, path, "cutoff_date")
    }
    error.getMessage should be("Source have not content")
  }

  it should "produce an exception when partition no exists" in {
    val path = "src/test/resources/data/input/t_mdco_branch_structure"

    val error = intercept[IOException] {
      HdfsUtil.listPartitionValues(spark, path, "cutoff_date1")
    }
    error.getMessage should be("Source have not partition with column: cutoff_date1")
  }

  ignore should "delete a path succesfully" in {
    val stringPath = "src/test/resources/data/input/delete/input.txt"
    assert(HdfsUtil.deletePath(spark, stringPath))
  }
}
