# Acceptance Tests for Spark projects

An acceptance test is a formal description of the behavior of a software product, generally expressed as an example or 
a usage scenario. The AT acts as a verification of the required business function and proper functioning of the system, 
emulating real-world usage conditions on behalf of customers.

``testAT`` module contains the source code and files for acceptance testing and defines the way other components are 
available for that purpose.

# testAT module components
* Acceptance tests resource files: this directory contains files used as resources in acceptance tests, such as 
feature files, data, process configuration and extra files to be included in the Spark execution context.
* Spark BDT library: included as a dependency, adds the main logic to make the acceptance tests work 
with a predefined common steps as well.
* Acceptance Test Runner: ``RunCukesTest.scala`` file is the one that performs the test execution.
* Custom steps: although ``spark-bdt` library includes some common steps and utilities, it is possible that a 
particular project might need to extend this and implement its own steps.

# Acceptance Tests resources
Resource files for acceptance tests should be included in ``src/test/resources`` relative path. Type of resources 
are defined in the following subfolders:
* ``feature``: here we define our test files using Gherkin language.
* ``data``: contains the data to be used in our tests.
* ``config``: directory to store config files for our process.
* ``sparkfiles``: files that will be included in the Spark context to be used in our process 
(example: ``caas-test.cfg`` for the tokenization file).

## Feature files
It contains the definition of our acceptance tests in Gherkin language. For now, some considerations are needed to 
make it work:
* Tags: Cucumber tags (``@tag``) are used in the feature file in order to include previously generated artifacts (jars). 
If you want to use your packaged process in the feature file, simply add a tag with the name of the module and it would 
be available in both, in the classpath and in the Spark context.
* File paths: As long as we don't use Docker container for acceptance tests anymore, paths to files in the defined 
steps must be targeted to local (relative) paths (example: ``src/test/resource/config/processExample.conf``)

# Spark BDT Library
This library, included as a dependency in our ``pom.xml`` file, performs two main actions:
* Initializes the test execution: add packaged jars from tags and sparkfiles to the Spark context 
and inialize it.
* Provides common steps to avoid multiple implementations of the same functionality.
* Sets ``Common`` Scala object used to share data between steps.

## Common Steps

### **Initialize process config path**

Initializes the dataframe metatada from the user supplied data.

```
Given a config file in path <configPath>
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| configPath | Local (relative) path to the process config file| ``.*`` |

### **Initialize from path**

Initializes the dataframe metatada from the user supplied data.

```
Given a dataframe located at path <fileUri> with alias <dfName> and config:
    | type   | schema.path   | delimiter   | header   | mode   |
    | <type> | <schema.path> | <delimiter> | <header> | <mode> |
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| fileUri | URI in which the dataframe is located. | ``.*`` |
| dfName | Alias used in other steps to refer to this dataframe. | ``\S+`` |
| type | File type to read (see [Kirby documentation](https://globaldevtools.bbva.com/bitbucket/projects/FJWAM/repos/kirby2/browse/doc/021_inputs.md?at=refs%2Ftags%2Fv2.8.1)). | ``.*`` |
| schema.path | URI in which the schema for this table is located. | ``.*`` |
| delimiter | CSV row value delimiter. By default: ``;``. | ``.*`` |
| header | Flag to indicate the input files have header or not (``true`` or ``false``, default ``false``). | ``.*`` |
| mode | Mode to deal with corrupt records when parsing the file (see [Kirby documentation](https://globaldevtools.bbva.com/bitbucket/projects/FJWAM/repos/kirby2/browse/doc/021_inputs.md?at=refs%2Ftags%2Fv2.8.1)). | ``.*`` |

### **Execute process**

Runs main class of the process (runProcess method).

```
execute main class <mainClass> in Spark
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| mainClass | Process main class (instance of InitSpark) to be executed. | ``.*`` |


### **Read Dataframe**

Reads a dataframe.

```
When I read <dfName> as dataframe
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias (to be referenced in later steps). | ``\S+`` |

### **Read Dataframe With Options**

Reads a dataframe with custom read options.

```
When I read <dfName> as dataframe with options
    | <option_name> | <option_value> |
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. If we read from a Kirby config file, we'll use ``input`` or ``output`` as alias. If we read from a path, we need to specify its corresponding alias. | ``\S+`` |
| option_name | Data source read option name to add or override. | .* |
| option_value | Data source read option value to add or override. | .* |

### **Filter Existing Dataframe**

Filters an already read dataframe and save it as a new dataframe to use it later on.

```
When I filter <dfName> dataframe with filter <filter> and save it as <newDfName>
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName    | Dataframe alias defined. If we read from a Kirby config file, we'll use ``input`` or ``output`` as alias. If we read from a path, we need to specify this alias. | ``\S+`` |
| filter    | Filter to apply to the dataframe ``dfName`` so we get ``newDfName``. | ``.*`` |
| newDfName | New dataframe alias. We'll use this alias to perform operations on this filtered dataframe in other steps. | ``\S+`` |

### **Read Dataframe With Filter**

Reads a dataframe applying a filter expression.

```
When I read <dfName> as dataframe with filter <filter> and save it as <newDfName>
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName    | Dataframe alias defined. If we read from a Kirby config file, we'll use ``input`` or ``output`` as alias. If we read from a path, we need to specify this alias. | ``\S+`` |
| filter    | Filter to apply to the dataframe ``dfName`` so we get ``newDfName``. | ``.*`` |
| newDfName | New dataframe alias. We'll use this alias to perform operations on this filtered dataframe in other steps. | ``\S+`` |

### **Check Dataframe Columns Between Dataframes**
Checks the number of columns in two dataframes.

```
Then the number of columns for <dfName1> dataframe is <equal to|more than|less than> the number of columns for <dfName2> dataframe
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName1 | Dataframe alias. | ``\S+`` |
| comparison | Comparison to be applied. | ``equals to`` \| ``more than`` \| ``less than`` |
| dfName2 | Dataframe alias to compare. | ``\S+`` |

### **Check Dataframe Columns**
Checks the number of columns in a dataframe.

```
Then the number of columns for <dfName> dataframe is <equal to|more than|less than> <columnsNumber>
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. | ``\S+`` |
| comparison | Comparison to be applied. | ``equals to`` \| ``more than`` \| ``less than`` |
| columnsNumber | Columns number to compare. | ``\d+`` |

### **Check Empty Dataframe**
Checks that the dataframe is not empty.

```
Then <dfName> dataframe is not empty
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. | ``\S+`` |

### **Check Dataframe Records**
Performs a comparison between records in two dataframes.

```
Then <dfName1> dataframe has <the same|less|more> records than <dfName2> dataframe
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName1 | Dataframe alias. | ``\S+`` |
| comparison | Comparison to be applied. | ``the same`` \| ``less`` \| ``more`` |
| dfName2 | Dataframe alias to compare. | ``\S+`` |

### **Check Duplicated Records in Columns**
Checks that the target dataframe doesn't have duplicated values for the given column(s).

```
Then <dfName> dataframe does not have duplicated values for columns:
    | column name   |
    | <column_name> |
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. | ``\S+`` |
| column_name | Column in which to look for duplicated records. | ``.*`` |

### **Check Duplicated Records in Dataframe**
Checks that target dataframe doesn't have duplicated values.

```
Then <dfName> dataframe does not have duplicated values
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. | ``\S+`` |

### **Check Null Values in Columns**
Checks that target dataframe doesn't have null values for the given column(s).

```
Then <dfName> dataframe does not have null values for columns:
    | column name   |
    | <column_name> |
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. | ``\S+`` |
| column_name | Column in which to look for null values. | ``.*`` |

### **Check Records Format**
Checks that the values in the target dataframe for the given column(s) have a specific format.

```
Then records for <dfName> dataframe have the format <format> for columns:
    | column name   |
    | <column_name> |
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. | ``\S+`` |
| format  | Regular expression to compare with. | ``.*`` |
| column_name | Column in which to look for null values. | ``.*`` |

### **Check Records Values in Range**
Checks that the records in the target dataframe for the given column(s) are between a range of values.

```
Then records for <dfName> dataframe are between <lowerBound> and <upperBound> for columns:
    | column name   |
    | <column_name> |
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. | ``\S+`` |
| lowerBound  | Min value in comparison range. | ``.*`` |
| upperBound  | Min value in comparison range. | ``.*`` |
| column_name | Column in which to look for null values. | ``.*`` |

### **Check Records Values Bounds**
Compares dataframe values for the given column(s) with a bound value.

```
Then records for <dfName> dataframe are <less|less or equal|equal|greater|greater or equal> than <boundValue> for columns:
    | column name   |
    | <column_name> |
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. | ``\S+`` |
| comparison  | Comparison to be applied. | ``less`` \| ``less or equal`` \| ``equal`` \| ``greater`` \| ``greater or equal`` |
| boundValue  | Value to compare with. | ``.*`` |
| column_name | Column in which to look for null values. | ``.*`` |

### **Check Catalog Values**
Compares the dataframe column values against some values from a catalog.

```
Then records for <dfName> dataframe in column <columnName> <have|do not have> the following values:
    | values     |
    | <value>    |
```
| **Parameters** | **Description** | **Regular Expression** |
| --- | --- | --- |
| dfName | Dataframe alias. | ``\S+`` |
| columnName  | Column name. | ``.*`` |
| comparison  | Check to be applied. | ``have`` \| ``do not have`` |
| value       | Catalog values. | ``.*`` |

## Common Object
It's pretty common to share states from one step to another. To do that, there is a Scala ``Common`` object available 
to be used. That object already have the following variables:
* Common.dfMap (Map\[String, DataFrame\]): stores the read dataframe for an specified alias.
* Common.spark (SparkSession): the initialized the Spark session.
* Common.genericMap (Map\[String, AnyRef\]): available to share any object between steps.

# Useful links
* [Cucumber](https://cucumber.io/)
* [Gherkin](https://docs.cucumber.io/gherkin/reference/)
