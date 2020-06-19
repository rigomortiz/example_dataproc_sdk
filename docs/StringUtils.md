## StringUtils

Using this object will allow you to work with some strings operations, below you can find each function explained:

### padRigth
____
Pads rigth a string with a element

#### usage   
   ```scala
    import com.bbva.datio.datahubpe.utils.commons.StringUtils.StringImplicits
     val string: String = "1"
     string.padRigth(2, "0")
    
```
This will return a string

|**Parameter**|**Description**|
|-------------|---------------|
|length|The new (minimum) length of the string|
|element|element|


### padLeft
____

Pads left a string ith a element
 

#### usage
      
```scala
   import com.bbva.datio.datahubpe.utils.commons.StringUtils.StringImplicits
       val string: String = "1"
       string.padLeft(2, "0")
```
      
This will return a boolean    

|**Parameter**|**Description**|
|-------------|---------------|
|length|The new (minimum) length of the string|
|element|element|

### asList
____

Return a List of elements separeted with  ,
#### usage
      
```scala
   import com.bbva.datio.datahubpe.utils.commons.StringUtils.StringImplicits
         val string: String = "1,2,3,4"
         string.asList()

```
      
This will return a list   




### ToDate
____
Convert a string a Date with a long format  

#### usage

```scala
           import com.bbva.datio.datahubpe.utils.commons.StringUtils.StringImplicits
           val string: String = "2020-01-24"
           val date = string.toDate()
    
```
      
This will return a list of partitions


### ToDate
____
Convert a string a Date with a format  

#### usage

```scala
    import com.bbva.datio.datahubpe.utils.commons.StringUtils.StringImplicits
    val Format         = "yyyy-MM-dd"           
    val string: String = "2020-01-24"
    val date = string.toDate(Format)
    
```
      
This will return Date with a format
|**Parameter**|**Description**|
|-------------|---------------|
|format|format|