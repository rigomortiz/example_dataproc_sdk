package com.bbva.datioamproduct.exampleTemplate

import com.bbva.datioamproduct.exampleTemplate.transformations.ConcreteTransformer
import com.bbva.datioamproduct.utils.data.{DataReader, DataWriter}
import com.bbva.datioamproduct.utils.flow.{FlowInitSpark, Transformer}


trait exampleTemplateTrait extends FlowInitSpark {

  override def getTranformer(): Transformer[DataReader, DataWriter] = new ConcreteTransformer(config)


}

object exampleTemplateJob extends exampleTemplateTrait with FlowInitSpark
