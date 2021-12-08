package com.bbva.datio.datahubpe.exampleTemplate

import com.bbva.datio.datahubpe.exampleTemplate.transformations.ConcreteTransformer
import com.bbva.datio.datahubpe.utils.processing.data.{DataReader, DataWriter}
import com.bbva.datio.datahubpe.utils.processing.flow.{FlowInitSpark, JobLauncher, Reader, Transformer}
import com.typesafe.config.Config

class ExampleSparkProcess extends FlowInitSpark {
  override def getTransformer(config: Config): Transformer[DataReader, DataWriter] = new ConcreteTransformer(config)

  override protected def getReader(config: Config): Reader[DataReader] = super.getReader(config)

  override def getProcessId: String = "ExampleSparkProcess"
}

object JobRunner extends JobLauncher {
  override val sparkProcessClass = "ExampleSparkProcess"
}
