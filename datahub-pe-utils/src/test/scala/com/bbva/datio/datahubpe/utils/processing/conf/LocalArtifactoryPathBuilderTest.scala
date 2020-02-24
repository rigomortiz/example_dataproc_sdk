package com.bbva.datio.datahubpe.utils.processing.conf

import org.scalatest.{FlatSpec, Matchers}

class LocalArtifactoryPathBuilderTest extends FlatSpec with Matchers {
  behavior of "Local Artifactory Path Builder"

  it should " build a path for local" in {
    val path         = "src/path/"
    val builder      = new LocalArtifactoryPathBuilder()
    val actualPath   = builder.build(path)
    val expectedPath = "src/path/"
    actualPath should be(expectedPath)
  }
}
