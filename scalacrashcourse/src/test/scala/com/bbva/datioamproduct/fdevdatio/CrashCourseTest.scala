package com.bbva.datioamproduct.fdevdatio


import com.datio.dataproc.sdk.api.context.RuntimeContext
import com.typesafe.config.Config
import org.scalatest.{BeforeAndAfterEach, FunSuite, Matchers}


class CrashCourseTest extends FunSuite with Matchers with ContextProvider with BeforeAndAfterEach {

  test("should test CrashCourse") {

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
