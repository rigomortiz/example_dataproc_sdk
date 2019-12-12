package com.bbva.datioamproduct.utils.flow

trait Transformer[R,W] {
  def transform(dataReader:R):W

}
