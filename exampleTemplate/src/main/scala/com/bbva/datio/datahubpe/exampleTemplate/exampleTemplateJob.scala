package com.bbva.datio.datahubpe.exampleTemplate

import com.bbva.datio.datahubpe.exampleTemplate.transformations.ConcreteTransformer
import com.bbva.datio.datahubpe.utils.processing.data.{DataReader, DataWriter}
import com.bbva.datio.datahubpe.utils.processing.flow.{FlowInitSpark, Transformer}
import com.typesafe.config.Config

object exampleTemplateJob extends FlowInitSpark {
  override def getTransformer(config: Config): Transformer[DataReader, DataWriter] =
    new ConcreteTransformer(config)
}
