package com.trace3.KafkaPubSub;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Scanner;

/**
 * Created by rmunoz on 1/23/17.
 */
public class Producer {

    private static Scanner in;

    public static void main(String[] argv) throws Exception {
        String host = argv[0];
        String topicName = argv[1];
        System.out.println("Writing to topic: " + topicName);
        in = new Scanner(System.in);
        Properties configProps = new Properties();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":9092");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        org.apache.kafka.clients.producer.Producer producer =
                new KafkaProducer<String,String>(configProps);
        String line = in.nextLine();
        while(!line.equals("exit")){
            ProducerRecord<String,String> rec = new ProducerRecord<String, String>(topicName,line);
            producer.send(rec);
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }


}
