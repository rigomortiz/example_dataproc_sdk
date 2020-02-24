package com.bbva.datio.datahubpe.utils.commons
import java.util.Calendar

import com.bbva.datio.datahubpe.utils.commons.StringUtils.StringImplicits
import org.scalatest.{FlatSpec, Matchers}

class StringUtilsTest extends FlatSpec with Matchers {
  behavior of "Pad Left"
  it should "return same element when length is 1" in {
    val string: String = "1"
    string.padLeft(1, "0") should be("1")
  }
  it should "return 01 when length is 2" in {
    val string: String = "1"
    string.padLeft(2, "0") should be("01")
  }
  behavior of "Pad Rigth"
  it should "return same element when length is 1" in {
    val string: String = "1"
    string.padRigth(1, "0") should be("1")
  }
  it should "return 10 when length is 2" in {
    val string: String = "1"
    string.padRigth(2, "0") should be("10")
  }

  behavior of "To list"
  it should "return 1 element" in {
    val string: String = "1"
    string.asList() should contain("1")
  }

  it should "return 2 elements" in {
    val string: String = "1,2"

    string.asList() should contain inOrder ("1", "2")
  }

  behavior of "To Date"
  it should "return a Calendar with format yyyy-MM-dd" in {
    val Format         = "yyyy-MM-dd"
    val string: String = "2020-01-24"

    val date = string.toDate(Format)

    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.get(Calendar.DATE) should be(24)
    calendar.get(Calendar.MONTH) should be(Calendar.JANUARY)
    calendar.get(Calendar.YEAR) should be(2020)
  }

  it should "return a Calendar with format yyyyMMdd" in {
    val Format: String = "yyyyMMdd"
    val string: String = "20200124"

    val date = string.toDate(Format)

    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    calendar.get(Calendar.DATE) should be(24)
    calendar.get(Calendar.MONTH) should be(Calendar.JANUARY)
    calendar.get(Calendar.YEAR) should be(2020)
  }
}
