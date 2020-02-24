package com.bbva.datio.datahubpe.utils.processing.conf

import org.scalatest.{FlatSpec, Matchers}

class WorkArtifactoryPathBuilderTest extends FlatSpec with Matchers {
  behavior of "Work Artifactory Path Builder"

  it should " build a path for work" in {
    val path       = "${repository.endpoint}/${schemas.repo}/${schemas.base-path}/path/"
    val builder    = new WorkArtifactoryPathBuilder()
    val actualPath = builder.build(path)
    val expectedPath = "https://artifactory-flegetonte.live.mx.ether.igrupobbva/artifactory/" +
      "da-datio-dev/" +
      "schemas/pe" +
      "/path/"
    actualPath should be(expectedPath)
  }
}
