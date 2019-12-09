@templateprocessing
Feature: Feature for Example

  Scenario: Test Example should result OK
    #Library Steps
    Given a config file in path src/test/resources/config/Example.conf
    When execute main class com.bbva.datioamproduct.datahubutils.Example in Spark
    Then exit code is equal to 0
    Given a dataframe located at path src/test/resources/data/wordcount-output with alias output and config:
      | type      |
      | parquet   |
    When I read output as dataframe
    Then the number of columns for output dataframe is equal to 2
    And records for output dataframe have the format \d+ for columns:
      | column name |
      | count       |
    And records for output dataframe have the format \S+ for columns:
      | column name |
      | word        |
    #Custom Step
    And and word has exactly 11 matches in output dataframe

  Scenario Outline: Test custom implementations result OK
    Given a config file in path src/test/resources/config/<configName>.conf
    When execute main class com.bbva.datioamproduct.datahubutils.Example in Spark
    Then exit code is equal to <expectedOutputCode>

      Examples:
        | configName     | expectedOutputCode |
        | Example | 0                  |
