## How works?


if you need to use the archetype, you must first include the dependency to your project

   ```xml
          <dependency>
              <groupId>com.bbva.datio.datahub-pe</groupId>
              <artifactId>datahub-pe-utils</artifactId>
              <version>0.3.3-SNAPSHOT</version>
          </dependency>
   
```
After this you must write your configuration file with the following tags:

- Inputs 
```editorconfig
          inputs {
            manager {
              paths = ["exampleTemplate/src/test/resources/data/input/t_mdco_tcom_manager"]
              type = parquet
              information_date = 2018-10-23
            }
            structure {
              paths = ["exampleTemplate/src/test/resources/data/input/t_mdco_branch_structure"]
              type = parquet
              cutoff_date = 2018-10-01
            }
        
          }
```
- outputs
 ```editorconfig
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
   
```
### Schema Configuration
____

First of all ,you must have the schemas in artifactory,After that you must add these variables before the schema path

**${repository.endpoint}/${schemas.repo}/${schemas.base-path}**

To configure the artifactory environments you need to set this tag

#### live
 ```editorconfig
    params{
     environment="live"
    }
```
#### work
 ```editorconfig
    params{
     environment="work"
    }
```
#### local
 ```editorconfig
    params{
     environment="local"
    }
```
