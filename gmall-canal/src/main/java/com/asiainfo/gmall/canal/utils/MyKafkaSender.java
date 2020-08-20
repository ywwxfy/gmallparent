package com.asiainfo.gmall.canal.utils;

import com.asiainfo.gmall.common.constants.GmallConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

//kafkaSend工具类
public class MyKafkaSender {

    public static void sendMessage(String topic,String message) throws Exception {
        KafkaProducer<String, String> producer = Create_Kafka_Producer();
        if (producer==null){
            System.out.println("创建producer 失败");
            throw new Exception("创建kafka producer 失败");
        }else{

            producer.send(new ProducerRecord<String, String>(topic, message));
        }

    }

    private static KafkaProducer<String,String>  Create_Kafka_Producer() {
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", GmallConstants.KAFKA_BOOTSTRAP_SERVER);
        prop.setProperty("key.serializer", GmallConstants.KAFKA_SERIALIZER);
        prop.setProperty("value.serializer", GmallConstants.KAFKA_SERIALIZER);
        KafkaProducer<String, String> kafkaProducer=null;
        try {
           kafkaProducer = new KafkaProducer<>(prop);
           return kafkaProducer;
        } catch (Exception e) {
            e.printStackTrace();
            return  null;
        }

    }

}
