package com.bbva.datio.datahubpe.utils.commons

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object StringUtils {
  implicit class StringImplicits(value: String) {
    /**
      *Pads rigth a string with a element
      *
      * @param length The new (minimum) length of the string
      * @param element elemnt
      * @return pads elements
      */
    def padRigth(length: Int, element: String): String = {
      val count: Int = length - value.length
      value + element * count
    }

    /**
      * Pads left a string ith a element
      *
      * @param length The new (minimum) length of the string
      * @param element elemnt
      * @return pads elements
      *
      */
    def padLeft(length: Int, element: String): String = {
      val count: Int = length - value.length
      element * count + value
    }

    /**
      * Return a List of elements separeted with  ,
      *
      * @return a List of strings
      */
    def asList(): List[String] = value.split(',').toList

    /**
      * Convert a string a Date with a long format
      *
      * @return a date with format
      */
    def ToDate(): Date = toDate("yyyy-MM-dd")

    /**
      * Convert a string a Date with a format
      *
      * @param format format
      * @return a calendar
      */
    def toDate(format: String): Date = {
      val calendar         = Calendar.getInstance
      val simpleDateFormat = new SimpleDateFormat(format)
      calendar.setTime(simpleDateFormat.parse(value))
      calendar.getTime
    }
  }
}
