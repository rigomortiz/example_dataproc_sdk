package com.bbva.datio.datahubpe.utils.processing.flow

trait Reader[R] {
  def read(): R
}
