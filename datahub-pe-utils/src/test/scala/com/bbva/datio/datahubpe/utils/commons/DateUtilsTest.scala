package com.bbva.datio.datahubpe.utils.commons

import java.util.{Calendar, Date}

import com.bbva.datio.datahubpe.utils.commons.StringUtils.StringImplicits
import com.bbva.datio.datahubpe.utils.commons.DateUtils.DateImplicits
import org.scalatest.{FlatSpec, Matchers}

class DateUtilsTest extends FlatSpec with Matchers {
  behavior of "First day of month"
  it should "return 1st day of months january" in {
    val Format         = "yyyy-MM-dd"
    val string: String = "2020-01-24"
    val date: Date     = string.toDate(Format)

    val result = date.getFirstDayOfMonth

    val calendar = Calendar.getInstance
    calendar.setTime(result)
    calendar.get(Calendar.DATE) should be(1)
    calendar.get(Calendar.MONTH) should be(Calendar.JANUARY)
    calendar.get(Calendar.YEAR) should be(2020)
  }
  behavior of "last day of month"
  it should "return last day of months january" in {
    val Format         = "yyyy-MM-dd"
    val string: String = "2020-01-24"
    val date: Date     = string.toDate(Format)

    val result = date.getLastDayOfMonth

    val calendar = Calendar.getInstance
    calendar.setTime(result)
    calendar.get(Calendar.DATE) should be(31)
    calendar.get(Calendar.MONTH) should be(Calendar.JANUARY)
    calendar.get(Calendar.YEAR) should be(2020)
  }
}
