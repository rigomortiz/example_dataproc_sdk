package com.bbva.datio.datahubpe.utils.processing.flow

trait Writer[W] {
  def write(dataWriter: W): Unit
}
