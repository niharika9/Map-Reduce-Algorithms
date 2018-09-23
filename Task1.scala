import java.io.{BufferedWriter, FileWriter}

import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import au.com.bytecode.opencsv.CSVWriter
import com.sun.prism.Texture.Usage

import scala.collection.JavaConverters._

object Task1 {

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }


  def main(args: Array[String]) : Unit = {
    // setting up a simple spark application
    val spark_config = new SparkConf().setAppName("Assignment-Task 1").setMaster("local[1]")
    val spark_context = new SparkContext(spark_config)


   val inputcsv = spark_context.textFile(args(0)).cache()
    val withoutHeader: RDD[String] = dropHeader(inputcsv)

    val partitions: RDD[(String, Int)] = withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.filter(line => {
        val columns = parser.parseLine(line)
        columns(52) != "NA" && columns(52) != "0"
      }).map(line => {
        val columns = parser.parseLine(line)
        (columns(3), 1)
      })
    })
    val total = partitions.count()

    val outputFile = new BufferedWriter(new FileWriter(args(1)))
    val csvWriter = new CSVWriter(outputFile)
    val totalval =  Array("Total",total.toString)

    var listOfRecords= List(totalval)
    val counts = partitions.
      reduceByKey {case (x,y) => x + y}.
      sortByKey(numPartitions = 1)   // repartitioned to 1 so that sort works properly
      .map{case (key, value) => Array(key, value)}.collect()

    csvWriter.writeAll(listOfRecords.asJava)
    for(l <- counts){
      val  x = Array(l(0).toString,l(1).toString)
      val x_list  = List(x)
        csvWriter.writeAll(x_list.asJava)
    }
    outputFile.close()

  }
}
