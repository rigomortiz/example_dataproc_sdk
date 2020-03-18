package com.bbva.datio.datahubpe.utils.processing.conf

import com.bbva.datio.datahubpe.utils.processing.conf.ArtifactoryPathFactory.EnvironmentType
import com.datio.kirby.constants.ConfigConstants.SCHEMA_CONF_KEY
import com.typesafe.config.ConfigFactory
import org.scalatest.{FlatSpec, Matchers}

class SchemaConfigValidatorTest extends FlatSpec with Matchers {
  behavior of "Schema Config Validator"

  it should " replace Labes" in {
    val csvFile =
      """{
        |        options {
        |            delimiter="|"
        |            header=true
        |            mode=FAILFAST
        |            charset="windows-1252"
        |            castMode=notPermissive
        |        }
        |        paths=[
        |            "/in/staging/ratransmit/external/pext/campaign_catalog_"${?DATE}"_"${?EXECUTION}".csv"
        |        ]
        |        schema {
        |            path="${repository.endpoint}/${schemas.repo}/${schemas.base-path}/kpfm/master/receiptissuerdetails/latest/receiptissuerdetails.output.schema"
        |        }
        |        type=csv
        |    }
          """.stripMargin
    val config = ConfigFactory.parseString(csvFile)

    val schemaConfigValidator = new SchemaConfigValidator(config)
    val expectedConfig        = schemaConfigValidator.replaceLabels(EnvironmentType.Work)
    val stringPath            = expectedConfig.getString(s"$SCHEMA_CONF_KEY.path")
    println(stringPath)
    stringPath should not contain ("${repository.endpoint}")
    stringPath should not contain ("${schemas.repo}")
    stringPath should not contain ("${schemas.base-path}")
  }

  it should " not replace Labes because no contains label Schema.Path" in {
    val csvFile =
      """{
        |        options {
        |            delimiter="|"
        |            header=true
        |            mode=FAILFAST
        |            charset="windows-1252"
        |            castMode=notPermissive
        |        }
        |        paths=[
        |            "/in/staging/ratransmit/external/pext/campaign_catalog_"${?DATE}"_"${?EXECUTION}".csv"
        |        ]
        |        type=csv
        |    }
          """.stripMargin
    val actualConfig = ConfigFactory.parseString(csvFile)

    val schemaConfigValidator = new SchemaConfigValidator(actualConfig)
    val expectedConfig        = schemaConfigValidator.replaceLabels(EnvironmentType.Work)
    actualConfig should be(expectedConfig)
  }
}
