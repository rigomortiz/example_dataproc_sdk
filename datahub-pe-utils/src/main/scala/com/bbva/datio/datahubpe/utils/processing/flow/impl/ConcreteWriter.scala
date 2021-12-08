package com.bbva.datio.datahubpe.utils.processing.flow.impl

import com.bbva.datio.datahubpe.utils.processing.conf.SchemaConfigValidator
import com.bbva.datio.datahubpe.utils.processing.data.DataWriter
import com.bbva.datio.datahubpe.utils.processing.flow.Writer
import com.datio.dataproc.kirby.api.Output
import com.datio.dataproc.kirby.core.actions.{DynamicDatasetSchema, Encrypt, InternalKirbyAction, Repartition, ReprocessPartitions, SafeReprocess, Validation}
import com.datio.dataproc.kirby.core.config.configurable.KirbyConfigurableFactory
import com.datio.dataproc.kirby.core.config.exception.MissingMandatoryOutputSchemaException
import com.typesafe.config.Config
import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}

class ConcreteWriter(config: Config) extends Writer[DataWriter] {
  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val outputFactory = new KirbyConfigurableFactory[Output]()

  override def write(dataWriter: DataWriter): Unit = {
    val outputKeyConfigReader = new OutputKeyConfigReader(config)
    val schemaConfigValidator = new SchemaConfigValidator(config)

    outputKeyConfigReader
      .getKeys()
      .foreach(key => {
        val itemWriter = dataWriter.getItemWriter(key)
        val acceptableOutputConf: Config = schemaConfigValidator.replaceLabels(outputKeyConfigReader.path + "." + key)

        val safeReprocess = SafeReprocess(acceptableOutputConf)
        val schema = Some(Option(outputFactory.getImplementation(acceptableOutputConf).getSchema.orElse(None.orNull))
          .getOrElse(throw MissingMandatoryOutputSchemaException()))

        val outputInternalActions: Seq[InternalKirbyAction] = Seq(
          Encrypt(schema, safeReprocess, None.orNull),
          safeReprocess.prepareOutputAction(),
          Repartition(acceptableOutputConf),
          DynamicDatasetSchema(acceptableOutputConf, schema),
          Validation(schema),
          ReprocessPartitions(acceptableOutputConf)
        )

        logger.debug("Apply pre output internal actions [Write Options and Schema validation]")
        val validatedOutputDf: DataFrame = outputInternalActions.foldLeft(itemWriter.df)((ds, action) => action.doAction(ds))

        outputFactory.getImplementation(
          outputInternalActions.foldLeft(acceptableOutputConf)((c, a) => a.prepareConfig(c))
        ).write(validatedOutputDf)

      })
  }
}
