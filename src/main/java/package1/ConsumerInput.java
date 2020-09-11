package package1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.concurrent.ExecutionException;


import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerInput {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //System.out.println("hello world!");

        Logger logger = LoggerFactory.getLogger(ConsumerInput.class.getName());

        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "inputt_topicc";

        Properties propertiesCon = new Properties();
        Properties propertiesProd = new Properties();

        propertiesProd.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        propertiesProd.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());
        propertiesProd.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(propertiesProd);
        //create consumer configs
        propertiesCon.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        propertiesCon.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesCon.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        propertiesCon.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        propertiesCon.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest/latest/none

        //create a consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(propertiesCon);

        //subscribe consumer to our topic(s)

        consumer.subscribe(Arrays.asList(topic));  //Arrays.asList("first_topic","second_topic"...) for multiple topics //Collections.singleton(topic) for single topic

        //poll for new data
        while (true){
           ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

           for(ConsumerRecord<String,String> record : records){

               String value = record.value();

               String key = record.key();

               if(value.contains("Pass")) {
                   String topicOutput = "outputt_topic";
                   ProducerRecord<String, String> recordProd = new ProducerRecord<String, String>(topicOutput, key, value);

                   logger.info("Keys : " + key); //log the key

                   //send data - asynchronous
                   producer.send(recordProd, new Callback() {
                       public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                           //executes every time a record is successfully sent or an exception is thrown
                           if (e == null) {
                               //record was successfully sent
                               logger.info("Received new metadata \n" + "Topic:" + recordMetadata.topic()
                                       + "\n" + "Partition:" + recordMetadata.partition() + "\n Offset: " + recordMetadata.offset() + "\n Timestamp" +
                                       recordMetadata.timestamp());
                           } else {
                               // e.printStackTrace();
                               logger.error("Error while producing", e);
                           }
                       }
                   }).get();
               }
               logger.info("Key: "+record.key()+", Value: "+record.value());
               logger.info("Partition: "+record.partition()+", Offset: "+record.offset());

           }
            producer.flush();
            //flush and close producer
            producer.close();
        }


    }
}
