Feature: Validate the trending top output when parameters top, country and year are set
  As CTO of Google, I want to know the trending top list per
  country, year, top and category "Most viewed video", "Most liked video" and "Most hated video"
  In order to prepare my Youtube Global Awards event in the country, year and top I set as parameters,
  so that I will have a table with the top of the three categories

  Scenario: Test CrashCourse should result OK
    Given The id of the process as 'CrashCourse'
    And A config file with the contents:
      """
      CrashCourse {
        input {
          t_fdev_channels="src/test/resources/input/youtube/t_fdev_channels"
          t_fdev_video_info = "src/test/resources/input/youtube/t_fdev_video_info"
          parameters {
            top = 5
            country = "MX"
            year = 2018
          }
        }
        output {
          path="src/test/resources/output/t_fdev_trending"
          schema {
            path="src/test/resources/schemas/t_fdev_trending.output.schema"
          }
        }
      }
      """
    When Executing the Launcher
    Then The exit code should be 0

  Scenario Outline: Validate the output has the top 10 for each category
    Given a dataframe located at path src/test/resources/output/t_fdev_trending with alias trendingDf
    And a Datio schema located at path src/test/resources/schemas/t_fdev_trending.output.schema with alias outputSchema
    When I read trendingDf as dataframe with Datio schema outputSchema
    And I filter trendingDf dataframe with filter <filter> and save it as <alias>
    Then records for <alias> dataframe are equal to 5

    Examples:
      | filter             | alias    |
      | category_event = 1 | viewedDf |
      | category_event = 2 | likedDf  |
      | category_event = 3 | hatedDf  |

  Scenario: The output has the correct structure and it is not empty for the filters
    Given a dataframe located at path src/test/resources/output/t_fdev_trending with alias trendingDf
    And a Datio schema located at path src/test/resources/schemas/t_fdev_trending.output.schema with alias outputSchema
    When I read trendingDf as dataframe with Datio schema outputSchema
    Then trendingDf dataframe is not empty
    And the number of columns for trendingDf dataframe is equal to 13

  Scenario: The output has the correct records for the filters
    Given a dataframe located at path src/test/resources/output/t_fdev_trending with alias trendingDf
    And a Datio schema located at path src/test/resources/schemas/t_fdev_trending.output.schema with alias outputSchema
    When I read trendingDf as dataframe with Datio schema outputSchema
    Then records for trendingDf dataframe in column video_id have the following values:
      | values      |
      | _I_D_8Z4sJE |
      | 9jI-z9QN6g8 |
      | kLpH1nSLJSs |
      | wfWkmURBNv8 |
      | VYOjWnS4cMY |
      | oWjxSkJpxFU |
      | VYOjWnS4cMY |
      | 84LBjXaeKk4 |
      | kLpH1nSLJSs |
      | 7C2z4GqqS5E |
      | 7C2z4GqqS5E |
      | VYOjWnS4cMY |
      | xpVfcZ0ZcFM |
      | kLpH1nSLJSs |
      | ffxKSjUwKdU |
