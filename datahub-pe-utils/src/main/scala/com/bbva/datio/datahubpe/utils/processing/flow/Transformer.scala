package com.bbva.datio.datahubpe.utils.processing.flow

trait Transformer[R, W] {
  def transform(dataReader: R): W

}
