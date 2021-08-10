package com.bbva.datioamproduct.fdevdatio.common

object StaticVal {

  // CONFIG FILE LEVELS
  val ROOT_CONFIG = "CrashCourse"
  val INPUT_CONFIG = s"$ROOT_CONFIG.input"
  val OUTPUT_CONFIG = s"$ROOT_CONFIG.output"
  val OUTPUT_SCHEMA = s"$ROOT_CONFIG.output.schema.path"
  val OUTPUT_PATH = s"$ROOT_CONFIG.output.path"

  val CHANNELS_TABLE = s"$INPUT_CONFIG.t_fdev_channels"
  val VIDEO_INFO = s"$INPUT_CONFIG.t_fdev_video_info"

  val PARAMETERS_TOP = s"$INPUT_CONFIG.parameters.top"
  val PARAMETERS_COUNTRY = s"$INPUT_CONFIG.parameters.country"
  val PARAMETERS_YEAR = s"$INPUT_CONFIG.parameters.year"

  // CATEGORIES FOR TOP CHANNELS
  val MOST_VIEWED = 1
  val MOST_LIKED = 2
  val MOST_HATED = 3

}
