package com.bbva.datioamproduct.fdevdatio

import com.datio.dataproc.sdk.launcher.SparkLauncher
import org.slf4j.{Logger, LoggerFactory}

object HelloWorldLauncher{
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      logger.info("Params config path is mandatory. Exiting...")
      System.exit(1000)
    }
    SparkLauncher.main(Array(args(0), "HelloWorldProcess"))
  }
}
