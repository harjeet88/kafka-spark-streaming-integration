package com.examples.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamingWordCount {
  def main(args: Array[String]) {


    // Create the spark streaming context with a 1 second batch size
    val sparkConf = new SparkConf().setAppName("Streaming word count")
    val ssc = new StreamingContext(sparkConf, Seconds(1))


    val dstreams = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    val words = lines.flatMap(x => x.split(" "))
    val wc = words.map(x => (x, 1)).reduceByKey(_ + _)
    wc.print()
    ssc.start()
    ssc.awaitTermination()
  }
} at