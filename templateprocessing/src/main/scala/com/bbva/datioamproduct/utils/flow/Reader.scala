package com.bbva.datioamproduct.utils.flow

trait Reader[R] {
  def read():R
}
