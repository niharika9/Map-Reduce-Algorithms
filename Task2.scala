import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.{CSVParser, CSVWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.Partitioner
import scala.collection.JavaConverters._

object Task2 {

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }

  class CountryPartitioner(numParts: Int) extends Partitioner {
    override def numPartitions: Int = numParts
    override def getPartition(key: Any): Int = {
      val domain = key
      val code = (domain.hashCode % numPartitions)
      if (code < 0) {
        code + numPartitions  // Make it non-negative
      } else {
        code
      }
    }
    // Java equals method to let Spark compare our Partitioner objects
    override def equals(other: Any): Boolean = other match {
      case dnp: CountryPartitioner =>
        dnp.numPartitions == numPartitions
      case _ =>
        false
    }
  }
  def main(args: Array[String]) : Unit = {
    // setting up a simple spark application
    val spark_config = new SparkConf().setAppName("Assignment-Task 2").setMaster("local[1]")
    val spark_context = new SparkContext(spark_config)

    val inputcsv = spark_context.textFile(args(0)).repartition(2)
    val withoutHeader: RDD[String] = dropHeader(inputcsv)

    /* Task 1 starts */
    val task1partitions: RDD[(String, Int)] = withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.filter(line => {
        val columns = parser.parseLine(line)
        columns(52) != "NA" && columns(52) != "0"
      }).map(line => {
        val columns = parser.parseLine(line)
        (columns(3), 1)
      })
    })
    val items1 = task1partitions.mapPartitions(iter => Array(iter.size).iterator, true).collect().toArray

    println("new output")
    val startTimeMillis1 = System.currentTimeMillis()
    task1partitions.reduceByKey((a, b) => a+b).sortByKey().collect()

    val endTimeMillis1 = System.currentTimeMillis()
    val durationSeconds1 = (endTimeMillis1 - startTimeMillis1)
    /* Task 1 ends */

    /* Task 2 starts */
    val task2partitions: RDD[(String, Int)] = withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.filter(line => {
        val columns = parser.parseLine(line)
        columns(52) != "NA" && columns(52) != "0"
      }).map(line => {
        val columns = parser.parseLine(line)
        (columns(3), 1)
      })
    }).partitionBy(new CountryPartitioner(2))
    val items2 = task2partitions.mapPartitions(iter => Array(iter.size).iterator, true).collect().toArray

    val startTimeMillis2 = System.currentTimeMillis()
    task2partitions.reduceByKey((a, b) => a+b).sortByKey().collect()
    val endTimeMillis2 = System.currentTimeMillis()
    val durationSeconds2 = (endTimeMillis2 - startTimeMillis2)
    /*Task 2 ends */


    /* Writing to output file */
    val outputFile = new BufferedWriter(new FileWriter(args(1)))
    val csvWriter = new CSVWriter(outputFile)

    val t1 = Array("standard",items1(0).toString,items1(1).toString,durationSeconds1.toString)
    val t1_list = List(t1)
    val t2 = Array("partition",items2(0).toString,items2(1).toString,durationSeconds2.toString)
    val t2_list = List(t2)
    csvWriter.writeAll(t1_list.asJava)
    csvWriter.writeAll(t2_list.asJava)
    outputFile.close()

  }
}
