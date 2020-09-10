package package1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys extends MysqlCon {
    public static String value;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //System.out.println("Hello World!");
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        String boostrapServers = "127.0.0.1:9092";
        ProducerDemoKeys obj = new ProducerDemoKeys();
        //create Producer Properties
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.server", boostrapServers); //hard coded
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        //properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create a producer record
        for(int i=0; i<10;i++) {

            String topic="input_topic";
            //String value="hello_world"+Integer.toString(i);

            String key="id_"+Integer.toString(i);


            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value );

            logger.info("Keys : "+key); //log the key

            //send data - asynchronous
            producer.send(record, new Callback() {
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
            }).get();  //block the .send() to make it synchronous-don't do this in production!!
        }
        //flush the data
        producer.flush();
        //flush and close producer
        producer.close();


    }
}
