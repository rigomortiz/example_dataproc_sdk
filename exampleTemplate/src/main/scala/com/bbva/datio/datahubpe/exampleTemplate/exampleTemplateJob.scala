package com.bbva.datio.datahubpe.exampleTemplate

import com.bbva.datio.datahubpe.exampleTemplate.transformations.ConcreteTransformer
import com.bbva.datio.datahubpe.utils.processing.data.{DataReader, DataWriter}
import com.bbva.datio.datahubpe.utils.processing.flow.{FlowInitSpark, Reader, Transformer}
import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

object exampleTemplateJob extends FlowInitSpark {
  override def getTransformer(config: Config): Transformer[DataReader, DataWriter] =
    new ConcreteTransformer(config)

  override protected def getReader(spark: SparkSession, config: Config): Reader[DataReader] =
    super.getReader(spark, config)
}
