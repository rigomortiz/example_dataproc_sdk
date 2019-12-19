package com.bbva.datioamproduct.utils.flow

trait Validator[W] {
  def validate(dataWriter:W):W
}
