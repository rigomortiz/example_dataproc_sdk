## HdfsUtil

Using this object will allow you to work with some Hdfs operations, below you can find each function explained:

### getLastPartition
____
Get the date of the last available partition of an hdfs path

#### usage   
   ```scala
    import com.bbva.datio.datahubpe.utils.commons.HdfsUtil
    val path               = "src/test/resources/data/input/t_mdco_branch_structure"
    val expectedPartitions = "2018-10-01"
    val actualPartitions   = HdfsUtil.getLastPartition(spark, path, "cutoff_date", "")
    
```
This will return a Date in format string

|**Parameter**|**Description**|
|-------------|---------------|
|spark|Sesion de spark
|path|Path of the object to get last partition
|fieldPartition|Partition field name
|date|check limit to get partition.


### existPath
____
return a value True if the path exists
 

#### usage
      
```scala
     import com.bbva.datio.datahubpe.utils.commons.HdfsUtil
     val path = "src/test/resources/data/input/t_mdco_branch_structure"
     HdfsUtil.existPath(path) 
```
      
This will return a boolean    

|**Parameter**|**Description**|
|-------------|---------------|
|path|path to evaluate|

### deletePath
____
Delete a path of hdfs
#### usage
      
```scala
     import com.bbva.datio.datahubpe.utils.commons.HdfsUtil
     val path = "src/test/resources/data/input/t_mdco_branch_structure"
     HdfsUtil.deletePath(path) 
```
      
This will return a boolean, true if delete path   


|**Parameter**|**Description**|
|-------------|---------------|
|path|path to delete|


### listPartitionValues
____
Get a list of partitions from an hdfs path   

#### usage

```scala
     val path = "src/test/resources/data/input/empty"
     val partitions =  HdfsUtil.listPartitionValues(spark, path, "cutoff_date")
    
```
      
This will return a List with the partitions of the entered route

|**Parameter**|**Description**|
|-------------|---------------|
|spark|Sesion de spark
|path|Path of the object to get last partition
|partition|Partition field name