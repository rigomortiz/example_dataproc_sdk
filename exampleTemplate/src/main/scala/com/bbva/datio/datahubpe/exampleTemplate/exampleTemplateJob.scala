package com.bbva.datio.datahubpe.exampleTemplate

import com.bbva.datio.datahubpe.exampleTemplate.transformations.ConcreteTransformer
import com.bbva.datio.datahubpe.utils.processing.data.{DataReader, DataWriter}
import com.bbva.datio.datahubpe.utils.processing.flow.{FlowInitSpark, Transformer}


trait exampleTemplateTrait extends FlowInitSpark {

  override def getTransformer(): Transformer[DataReader, DataWriter] = new ConcreteTransformer(config)


}

object exampleTemplateJob extends exampleTemplateTrait with FlowInitSpark
