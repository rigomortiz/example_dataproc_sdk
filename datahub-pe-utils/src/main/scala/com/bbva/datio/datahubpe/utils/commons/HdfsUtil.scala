package com.bbva.datio.datahubpe.utils.commons

import java.io.{FileNotFoundException, IOException}
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.functions.{col, lit, row_number}

object HdfsUtil {
  private val PartitionDate     = "partition_date"
  private val PartitionSortDate = "partition_sort_date"
  val FormatDate                = "date"
  val FormatHyphenDate          = "yyyy-MM-dd"

  /** Obtiene la fecha de la ultima particion disponible de un ruta hdfs
    *
    * @param path Ruta del objeto a obtener ultima particion
    * @param fieldPartition Nombre del campo de particion
    * @param date Fecha limite para obtener la particion.
    *             Si se envia obtendra la ultima particion teniendo como limite la fecha.
    *             Si no se envia obtendra la ultima particion ingestada.
    * @param spark Sesion de spark
    * @return Fecha de la ultima particion ingestada
    */
  def getLastPartition(spark: SparkSession, path: String, fieldPartition: String, date: String): String = {
    val partitionValues = listPartitionValues(spark, path, fieldPartition)
    date match {
      case "" =>
        partitionValues.max.toString
      case _ =>
        val processDate: Date = new Date(new SimpleDateFormat(FormatHyphenDate).parse(date).getTime)
        val partitionDf = spark
          .createDataFrame(partitionValues.toList.map(Tuple1(_)))
          .toDF(PartitionDate)
          .withColumn(fieldPartition, col(PartitionDate).cast(FormatDate))
          .filter(col(fieldPartition) <= lit(processDate))
          .drop(col(PartitionDate))
          .select(col(fieldPartition), row_number().over(orderBy(col(fieldPartition).desc)).alias(PartitionSortDate))
          .filter(col(PartitionSortDate) === lit(1))
          .drop(col(PartitionSortDate))
        partitionDf.first().getDate(0).toString
    }
  }

  /**
    * Function to write parquet when it's necesary a reprocess.
    *
    * @return Boolean (true: path hdfs exist)
    */
  def existPath(spark: SparkSession, stringPath: String): Boolean = {
    val hadoopFileSystem = createFileSystem(spark)
    val path             = createPath(stringPath)

    hadoopFileSystem.getFileStatus(path).isDirectory
  }

  /**
    * Function to delete path hdfs, it's necesary use try and catch
    *
    * @return retorna verdadero si elimino el path
    */
  def deletePath(spark: SparkSession, stringPath: String): Boolean = {
    val hdfsPath   = createPath(stringPath)
    val fileSystem = createFileSystem(spark)
    fileSystem.delete(hdfsPath, true)
  }

  def createPath(StringPath: String): Path = {
    new Path(StringPath)
  }

  /***
    *  crea un file sustem hadoop
    * @param spark session de spark
    * @return
    */
  def createFileSystem(spark: SparkSession): FileSystem = {
    val hadoopConfig = spark.sparkContext.hadoopConfiguration
    FileSystem.get(hadoopConfig)
  }

  /** Obtiene una lista con las particiones de una ruta hdfs
    *
    * @param spark Session de spark
    * @param path Ruta donde se encuentran las particiones
    * @param partition Nombre del campo de particion
    * @return Lista con las particiones de la ruta ingresada
    */
  def listPartitionValues(spark: SparkSession, path: String, partition: String): Seq[String] = {
    existPath(spark, path)

    val fileSystem = createFileSystem(spark)
    val parentPath = createPath(path)

    val files                       = fileSystem.listStatus(parentPath)
    val listFilesNames: Seq[String] = files.map(_.getPath().toString)

    if (listFilesNames.isEmpty) {
      throw new FileNotFoundException("Source have not content")
    }

    val pathLength = fileSystem.getFileStatus(parentPath).getPath.toString.length

    val partitions =
      listFilesNames.map(_.substring(pathLength + 1)).filter(_.contains(partition))

    if (partitions.isEmpty) {
      throw new IOException("Source have not partition with column: " + partition)
    }
    partitions.map(_.substring(partition.length + 1))
  }
}
