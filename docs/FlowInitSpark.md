## FlowInitSpark

Using this object will allow you to work with Init spark, below you can find each function explained:

### getTransformer
____
Get a transformer which allows you to follow the flow of the processing

#### usage   
   ```scala
object exampleTemplateJob extends FlowInitSpark {
  override def getTransformer(config: Config): Transformer[DataReader, DataWriter] =
    new ConcreteTransformer(config)
}
```

### getReader
____

Get a reader that allows you to implement a specific class for reading the sources

#### usage   
   ```scala
object exampleTemplateJob extends FlowInitSpark {
   override protected def getReader(spark: SparkSession, config: Config): Reader[DataReader] =
      super.getReader(spark, config)
}

    
```
This will return a string

|**Parameter**|**Description**|
|-------------|---------------|
|config|config|