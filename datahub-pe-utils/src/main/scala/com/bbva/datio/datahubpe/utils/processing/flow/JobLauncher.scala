package com.bbva.datio.datahubpe.utils.processing.flow

import com.datio.dataproc.sdk.launcher.SparkLauncher
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

trait JobLauncher {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val sparkProcessClass: String

  final def main(args: Array[String]): Unit = {
    Try {
      if (args.length == 0) {
        throw new Exception("Parameter configuration file path is mandatory. Exiting...")
      }
      SparkLauncher.main(Array(args(0), sparkProcessClass))
    } match {
      case Failure(exception) => {
        exception.printStackTrace()
        -1
      }
      case Success(_) => 0
    }
  }
}
