import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.{CSVParser, CSVWriter}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.BigDecimal
import scala.collection.JavaConverters._

object Task3{

  def dropHeader(data: RDD[String]): RDD[String] = {
    data.mapPartitionsWithIndex((idx, lines) => {
      if (idx == 0) {
        lines.drop(1)
      }
      lines
    })
  }


  def main(args: Array[String]) : Unit = {
    val spark_config = new SparkConf().setAppName("Assignment-Task 3").setMaster("local[2]")
    val spark_context = new SparkContext(spark_config)

    val inputcsv = spark_context.textFile(args(0)).cache()
    val withoutHeader: RDD[String] = dropHeader(inputcsv)

    val partitions: RDD[(String, Double)] = withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.filter(line => {
        val columns = parser.parseLine(line)
        columns(52) != "NA" && columns(52) != "0"
      }).map(line => {
        val columns = parser.parseLine(line)
        val make = if(columns(53) == "Weekly") 52 else if (columns(53) == "Monthly") 12 else 1
        val salary = columns(52).replaceAll(",","")
        val sal = salary.toDouble * make
        (columns(3), sal)
      })
    }).sortByKey(true)

    /* Calculating average for every country*/
    val avg_map = partitions.mapValues(value => (value,1)).
      reduceByKey{
        case((sumL,countL) , (sumR,countR)) =>
          (sumL + sumR ,countL + countR)
      }.sortByKey(true).
      mapValues{
        case (sum , count) => sum/count
      }
    val avg_mapreduce =  avg_map.sortByKey(numPartitions = 1).map{case (key,value) => (key,BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble)}



    /* Calculating count of nonzero salaries for every country*/
    val partitions1: RDD[(String, Int)] = withoutHeader.mapPartitions(lines => {
      val parser = new CSVParser(',')
      lines.filter(line => {
        val columns = parser.parseLine(line)
        columns(52) != "NA" && columns(52) != "0"
      }).map(line => {
        val columns = parser.parseLine(line)
        (columns(3), 1)
      })
    }).sortByKey(true)
    val counts_mapreduce = partitions1.
      reduceByKey {case (x,y) => x + y}.
      sortBy {case (key, value) => key} //.foreach(println)


    /* Calculating minimum of salary for every country*/
    val min_map = partitions.
      reduceByKey {case (x,y) => {
        val less = if (x < y) x else y
        less
      }}.sortBy {case (key, value) => key}
    val min_mapreduce = min_map.map{ case (key,value) => (key, value.toInt)}


    /* Calculating maximum of salary for every country*/
    val max_map = partitions.
      reduceByKey {case (x,y) => {
        val less = if (x > y) x else y
        less
      }}.sortBy {case (key, value) => key}
    val max_mapreduce = max_map.map{case (key,value) => (key, value.toInt)}


    /*Join the above four RDDs to get the final RDDs*/
    val joinedmapreduce = counts_mapreduce.join(min_mapreduce).join(max_mapreduce).join(avg_mapreduce).sortByKey(numPartitions = 1)
    val finalmap = joinedmapreduce.map{ case (key,(((value1,value2),value3),value4)) => Array(key,value1,value2,value3,value4)}.collect.toArray


    /*Write output to csv file*/
    val outputFile = new BufferedWriter(new FileWriter(args(1)))
    val csvWriter = new CSVWriter(outputFile)

    for(l <- finalmap){
      val x = Array(l(0).toString,l(1).toString,l(2).toString,l(3).toString,l(4).toString)
      val x_list = List(x)
      csvWriter.writeAll(x_list.asJava)
    }
    outputFile.close()
  }
}