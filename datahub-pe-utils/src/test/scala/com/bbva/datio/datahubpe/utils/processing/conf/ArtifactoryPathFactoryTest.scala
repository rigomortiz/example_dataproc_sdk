package com.bbva.datio.datahubpe.utils.processing.conf

import com.bbva.datio.datahubpe.utils.processing.conf.ArtifactoryPathFactory.EnvironmentType
import org.scalatest.{FlatSpec, Matchers}

class ArtifactoryPathFactoryTest extends FlatSpec with Matchers {
  behavior of "Artifactory Factory"

  it should " create a instance LocalArtifactoryPathBuilder" in {
    val actualInstance = ArtifactoryPathFactory(EnvironmentType.Local)

    actualInstance shouldBe a[LocalArtifactoryPathBuilder]
  }
  it should " create a instance WorkArtifactoryPathBuilder" in {
    val actualInstance = ArtifactoryPathFactory(EnvironmentType.Work)

    actualInstance shouldBe a[WorkArtifactoryPathBuilder]
  }
  it should " create a instance LiveArtifactoryPathBuilder" in {
    val actualInstance = ArtifactoryPathFactory(EnvironmentType.Live)

    actualInstance shouldBe a[LiveArtifactoryPathBuilder]
  }
  it should " return a exception when the environment is not validate" in {
    val error = intercept[IllegalArgumentException] {
      ArtifactoryPathFactory("others")
    }
    error.getMessage should be("Environment type incorrect, only accepted [local,live,work]")
  }
}
