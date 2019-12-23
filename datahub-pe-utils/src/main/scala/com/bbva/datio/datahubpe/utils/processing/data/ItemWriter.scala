package com.bbva.datio.datahubpe.utils.processing.data

import org.apache.spark.sql.DataFrame

case class ItemWriter(df:DataFrame,schemaValidation:Boolean)


