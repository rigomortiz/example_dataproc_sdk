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
