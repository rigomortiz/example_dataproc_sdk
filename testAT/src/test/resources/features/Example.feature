Feature: Feature for AmazonReports

  Scenario: Test Engine should return 0 in success execution
    Given a config file with path: src/test/resources/config/application-testAT.conf
    When I execute the process: ReadCsvProcess
    Then the exit code should be 0

  Scenario: Validate the output has not values S; XL in size column
    Given a dataframe outputDF in path: src/test/resources/data/output/t_fdev_customer_bikes
    When I filter outputDF dataframe with the filter: size IN ("S", "XL") and save it as outputFilteredDF dataframe
    Then outputFilteredDF dataframe has exactly 0 records

  Scenario: Validate the output has just values M, L in size
    When I filter outputDF dataframe with the filter: size IN ("M", "L") and save it as outputFilteredDF dataframe
    Then outputFilteredDF has exactly the same number of records than outputDF