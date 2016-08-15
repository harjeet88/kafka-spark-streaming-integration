package com.myproj

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import java.io.Serializable

class KafkaSink( createProducer : () => new KafkaProducer[String,String]) extends Serializable { lazy val prdcr = createProducer()

def send(topic : String, msg : String ): Unit ={ prdcr.send(new ProducerRecord[Strig,String] (topic,msg) )}
}  

// Create object of KafkaSink
object KafkaSink{
def apply(config:Map[String,Object]):KafkaSink = {
    val f = () =>{ val prdcr = new KafkaProducer[String,String](config)
    sys.addShutdownHook{
         prdcr.close()
    }
    prdcr
    }
    new KafkaSink(f)
}}