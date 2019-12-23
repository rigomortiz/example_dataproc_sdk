package com.bbva.datio.datahubpe.utils.processing.flow

trait Validator[W] {
  def validate(dataWriter: W): W
}
