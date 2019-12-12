package com.bbva.datioamproduct.utils.flow

trait Writer[W] {
def write(dataWriter:W):Unit
}
