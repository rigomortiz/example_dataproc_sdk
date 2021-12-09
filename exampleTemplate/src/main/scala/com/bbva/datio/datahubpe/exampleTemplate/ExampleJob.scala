package com.bbva.datio.datahubpe.exampleTemplate

import com.bbva.datio.datahubpe.utils.processing.flow.JobLauncher

object ExampleJob extends JobLauncher {
  override val sparkProcessClass: String = "ExampleSparkProcess"
}
