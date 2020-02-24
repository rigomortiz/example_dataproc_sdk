package com.bbva.datio.datahubpe.utils.processing.conf

import org.scalatest.{FlatSpec, Matchers}

class LiveArtifactoryPathBuilderTest extends FlatSpec with Matchers {
  behavior of "Live Artifactory Path Builder"

  it should " build a path for live" in {
    val path       = "${repository.endpoint}/${schemas.repo}/${schemas.base-path}/path/"
    val builder    = new LiveArtifactoryPathBuilder()
    val actualPath = builder.build(path)
    val expectedPath = "https://artifactory-flegetonte.live.mx.ether.igrupobbva/artifactory/" +
      "da-datio/" +
      "schemas/pe" +
      "/path/"
    actualPath should be(expectedPath)
  }
}
