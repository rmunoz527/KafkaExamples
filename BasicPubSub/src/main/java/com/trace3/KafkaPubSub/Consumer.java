package com.trace3.KafkaPubSub;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

/**
 * Created by rmunoz on 1/23/17.
 */
public class Consumer {
    private static Scanner in;
    private static boolean stop = false;

    public static void main(String[] argv) throws Exception{
        in = new Scanner(System.in);
        String host = argv[0];
        String topicName = argv[1];
        String groupId = argv[2];
        System.out.println("Topic name :" + topicName );
        System.out.println("GroupId :" + groupId);

        ConsumerThread consumerRunnable = new ConsumerThread(host,topicName,groupId);
        consumerRunnable.start();
        String line = "";
        while (!line.equals("exit")) {
            line = in.next();
        }
        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread{
        private String host;
        private String topicName;
        private String groupId;

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return kafkaConsumer;
        }

        private KafkaConsumer<String,String> kafkaConsumer;

        public ConsumerThread(String host, String topicName, String groupId){
            this.host = host;
            this.topicName = topicName;
            this.groupId = groupId;
        }

        public void run(){
            System.out.println("Listening at broker");
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, host + ":9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG,"simple");

            // Check on where to start processing messages from
            kafkaConsumer = new KafkaConsumer<String, String>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));


            // Process messages
            try{
                while(true){
                    ConsumerRecords<String,String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records)
                        System.out.println(record.value());
                }
            }
            catch(WakeupException ex){}
            finally{
                kafkaConsumer.close();
                System.out.println("Closed Kafka consumer");
            }
        }


    }
}


