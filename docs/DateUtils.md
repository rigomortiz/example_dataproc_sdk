## DateUtils

Using this object will allow you to work with some Date operations, below you can find each function explained:

### addMonths
____
Adds or subtracts the specified amount of months for example, to subtract 5 days from the current time, you can achieve it by calling:

#### usage   
   ```scala
   import com.bbva.datio.datahubpe.utils.commons.DateUtils.DateImplicits
   date.addMonths(-5)
   ```

This will return a Date

|**Parameter**|**Description**|
|-------------|---------------|
|amount|the amount of months to be added to Date

### addDays
____
Adds or subtracts the specified amount of dates for example, to subtract 5 days from the current time, you can achieve it by calling:

#### usage
      
```scala
   import com.bbva.datio.datahubpe.utils.commons.DateUtils.DateImplicits
   date.addDays(-5)
```
      
This will return a Date      

|**Parameter**|**Description**|
|-------------|---------------|
|amount|the amount of months to be added to Date|

### add
____
Adds or subtracts the specified amount of field for example, to subtract 5 days from the current time, you can achieve it by calling:

#### usage
      
```scala
   import com.bbva.datio.datahubpe.utils.commons.DateUtils.DateImplicits
   import java.util.Calendar   
   date.add(Calendar.DATE,-5)
   date.add(Calendar.MONTH,-5)
```
      
This will return a Date   


|**Parameter**|**Description**|
|-------------|---------------|
|field|field  the calendar field.|
|amount|the amount of field to be added to Date|

### getFirstDayOfMonth
____
Get the first day of month       
#### usage

```scala
   import com.bbva.datio.datahubpe.utils.commons.DateUtils.DateImplicits
   date.getFirstDayOfMonth()
```
      
This will return a Date   

### getLastDayOfMonth
____
Get the last day of month

#### usage       

```scala
   import com.bbva.datio.datahubpe.utils.commons.DateUtils.DateImplicits
   date.getLastDayOfMonth()
```
      
This will return a Date   
