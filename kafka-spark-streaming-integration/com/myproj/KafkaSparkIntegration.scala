package com.myproj

import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata}
import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka._
import com.myproj.KafkaSink
import org.apache.kafka.common.serialization.StringSerializer

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

import org.apache.spark.streaming.{Seconds, StreamingContext} 

object KafkaSparkIntegration{
     import scala.collection.JavaConversions._

    main(args:String[]):Unit =  {
       // Create the spark streaming context with a 1 second batch size
      val sparkConf = new SparkConf().setAppName("Streaming word count")
      val ssc = new StreamingContext(sparkConf, Seconds(5))

      val config = Map(
      "bootstrap.servers"="kafka-host:9092"
      "key.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer"
    )

      val kafkaSink = sc.bradcast(KafkaSink(config))

      @transient val kafkaStream = KafkaUtils.createStream(ssc,"zookpr-host:2181","kafka-grp", Map("Kafka_input" -> 1.toInt))
      
       kafkaStream.foreachRDD{ rdd => rdd.foreachPartition{ 
            part => part.foreach(msg => kafkaSink.value.send("kafka_out_topic",msg.toString)
            }}

      ssc.start()
      ssc.awaitTermination()
}

}

