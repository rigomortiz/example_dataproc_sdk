package com.bbva.datioamproduct.fdevdatio

import com.bbva.datioamproduct.fdevdatio.testUtils.{ContextProvider, FakeRuntimeContext}

class ReadCsvTest extends ContextProvider {

  "runProcess method" should "return 0 in success execution" in {
    val runtimeContext = new FakeRuntimeContext(config)
    val exitCode = new ReadCsvProcess().runProcess(runtimeContext)
    exitCode shouldBe 0
  }

}
