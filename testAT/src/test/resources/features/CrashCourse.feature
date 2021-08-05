Feature: Feature for CrashCourse
  Scenario: Run a service process
    Given The id of the process as 'CrashCourse'
    And A config file with the contents:
      """
      CrashCourse {
        key = value
      }
      """
    When Executing the Launcher
    Then The exit code should be 0
