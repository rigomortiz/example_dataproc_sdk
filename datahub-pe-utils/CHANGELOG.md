# Changelog
All notable changes to this project will be documented in this file.
## [0.3.5] - 2020-06-19

### Added

- update kirby version to 2.12.6
  
  
## [0.3.4] - 2020-06-19

### Added

- add Method getReader to FlowInitSpark 


## [0.3.3] - 2020-04-01

### Added

- New functions about  Hdfs Utils
1. getLastPartition
2. existPath
3. deletePath
4. createPath
5. createFileSystem
6. listPartitionValues

## [0.3.1] - 2020-02-25

### Added
### Changed
 - Add  ArtifactoryPathFactory to SchemaConfigValidator 
 ####Use
 You can configurate your Config file with the following code

 <pre><code> 
 appJob {
     params{
      environment="work"
     }
     outputs {
        beygmanager1 {
          mode = reprocess
          reprocess = ["information_date=2018-10-23"]
          coalesce {
            partitions = 1
          }
          partition = [
            "information_date"
          ]
          path = "exampleTemplate/src/test/resources/data/output/punct/t_mbmi_beyg_manager1"
          schema {
           path = "${repository.endpoint}/${schemas.repo}/${schemas.base-path}/kpfm/master/receiptissuerdetails/latest/receiptissuerdetails.output.schema"
          }
         type = parquet
        }
      } 
  </code></pre>

## [0.3.0] - 2020-02-24

### Added

- New functions about  Date Utils
- New functions about  String Utils

### Changed

- improve Reader Configuration Artifactory [Local,Work,Live]


### Removed

