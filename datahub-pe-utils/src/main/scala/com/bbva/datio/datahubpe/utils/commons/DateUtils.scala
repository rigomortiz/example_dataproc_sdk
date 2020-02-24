package com.bbva.datio.datahubpe.utils.commons

import java.util.{Calendar, Date}

object DateUtils {
  implicit class DateImplicits(value: Date) {
    val MONTH       = Calendar.MONTH
    val DATE: Int   = Calendar.DATE
    val SECOND: Int = Calendar.SECOND

    /**
      * Adds or subtracts the specified amount of months
      * For example, to subtract 5 days from
      * the current time, you can achieve it by calling:
      * <p><code>date.addMonths(-5)</code>.
      *
      * @param amount the amount of months to be added to Date
      * @return a date
      */
    def addMonths(amount: Int): Date = {
      add(MONTH, amount)
    }

    /**
      * Adds or subtracts the specified amount of dates
      * For example, to subtract 5 days from
      * the current time, you can achieve it by calling:
      * <p><code>date.addDays(-5)</code>.
      *
      * @param amount the amount of months to be added to Date
      * @return a date
      */
    def addDays(amount: Int): Date = {
      add(DATE, amount)
    }

    /**
      * @param field  the calendar field.
      * @param amount the amount of date or time to be added to the field.
      * @return
      */
    def add(field: Int, amount: Int): Date = {
      val calendar = Calendar.getInstance()
      calendar.setTime(value)
      calendar.add(field, amount)
      calendar.getTime()
    }

    def getFirstDayOfMonth(): Date = {
      val calendar = Calendar.getInstance()
      calendar.setTime(value)
      calendar.set(DATE, calendar.getMinimum(DATE))
      calendar.getTime()
    }
    def getLastDayOfMonth(): Date = {
      val calendar = Calendar.getInstance()
      calendar.setTime(value)
      calendar.set(DATE, calendar.getMaximum(DATE))
      calendar.getTime()
    }
  }
}
