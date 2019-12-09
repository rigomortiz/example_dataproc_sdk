# Datio Archetype Template for Spark projects. 

# README for Example

## Remove the next section and fill me with documentation 

# Getting started with the new project
## First Compilation

Make a clean install in the root directory of the project

```bash
mvn clean install
```

The testAT are commented, for run testAT uncomment the next line in `RunCukesTest.scala` file
```
@CucumberOptions(features = Array(
  "src/test/resources/features/Example.feature"
)
```

## First Execution

Go to your IDE run configurations window and set the next configuration:
* Enable the maven profile `run-local`
* Set the next VM Option =>  `-Dspark.master=local[*]`
* Set the main class => `com.bbva.datioamproduct.datahubutils.Example`
* Set as program argument a valid path to a configuration file (not empty)


Generated with spark-job-archetype version 2.0.0

# How to create a new project with Structured Streaming (Examples)

There are two repositories with a few examples with Structured Streaming:

* https://globaldevtools.bbva.com/bitbucket/projects/REOUO/repos/dataproc-epsilon-example/browse
* https://globaldevtools.bbva.com/bitbucket/projects/REOUO/repos/cadmo-transactions-example/browse

They use Cadmo for reading and writing streams from Upsilon. You can use it with Epsilon as well. This is the
documentation about those pieces:

* https://platform.bbva.com/en-us/developers/dataproc/documentation/dataproc-libraries-tools/epsilon-connector/what-is-epsilon-connector