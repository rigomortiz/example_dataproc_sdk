package com.bbva.datio.datahubpe.utils.processing.conf

class LiveArtifactoryPathBuilder extends ArtifactoryPathBuilder {
  override val RepositoryCacheEndPoint: String = "https://artifactory-flegetonte.live.mx.ether.igrupobbva/artifactory"
  override val SchemaRepository: String        = "da-datio"
  override val SchemaBasePath: String          = "schemas/pe"
}
