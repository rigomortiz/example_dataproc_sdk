package com.bbva.datioamproduct.fdevdatio


import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}
import com.datio.dataproc.sdk.launcher.process.config.ProcessConfigLoader


class CrashCourseTest extends FunSuite with Matchers with ContextProvider with BeforeAndAfterEach {

  test("should test CrashCourse") {
    // Given
    val config = new ProcessConfigLoader().fromPath("src/test/resources/config/application-test.conf")
    val runtimeContext = new FakeRuntimeContext(config)
    val jobConfig = config.getConfig("CrashCourse")

    // When
    val exitCode = new CrashCourse().runProcess(runtimeContext)

    // Then
    exitCode shouldBe 0
  }


  class FakeRuntimeContext(config: Config) extends RuntimeContext {

    override def getProcessId: String = "fake"

    override def getConfig: Config = config

    override def getProperty(propertyName: String): AnyRef = {
      throw new UnsupportedOperationException
    }

    override def setAdditionalInfo(additionalInfo: String): Unit = {}

    override def setMessage(message: String): Unit = {}

    override def setUserCode(userCode: String): Unit = {}
  }

}
