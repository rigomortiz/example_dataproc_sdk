package com.bbva.datio.datahubpe.utils.processing.conf
trait ArtifactoryPathBuilder {
  val RepositoryCacheEndPoint: String
  val SchemaRepository: String
  val SchemaBasePath: String
  final def build(path: String): String = {
    path
      .replace("${repository.endpoint}", RepositoryCacheEndPoint)
      .replace("${schemas.repo}", SchemaRepository)
      .replace("${schemas.base-path}", SchemaBasePath)
  }
}
