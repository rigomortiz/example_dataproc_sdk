package com.bbva.datio.datahubpe.utils.processing.conf

object ArtifactoryPathFactory {
  object EnvironmentType {
    val Local: String = "local"
    val Work: String  = "work"
    val Live: String  = "live"
  }
  def apply(environmentType: String): ArtifactoryPathBuilder = {
    environmentType match {
      case EnvironmentType.Local => new LocalArtifactoryPathBuilder()
      case EnvironmentType.Work  => new WorkArtifactoryPathBuilder()
      case EnvironmentType.Live  => new LiveArtifactoryPathBuilder()
      case _                     => throw new IllegalArgumentException("Environment type incorrect, only accepted [local,live,work]")
    }
  }
}
