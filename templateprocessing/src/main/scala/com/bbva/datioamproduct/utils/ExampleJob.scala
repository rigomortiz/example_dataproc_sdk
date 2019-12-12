package com.bbva.datioamproduct.utils
import flow.FlowInitSpark
import flow.Transformer
import flow.impl.ConcreteTransformer
import data.DataWriter
import data.DataReader


trait ExampleTrait extends FlowInitSpark{

  override def getTranformer(): Transformer[DataReader, DataWriter] = new ConcreteTransformer(config)

}
object ExampleJob extends ExampleTrait with FlowInitSpark