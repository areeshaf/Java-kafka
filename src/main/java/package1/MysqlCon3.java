package package1;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class MysqlCon3 {
    public static int st_id;
    public static String st_name;
    public static String st_status;

    public static void main(String args[]) throws ExecutionException, InterruptedException {
        try{
            Class.forName("com.mysql.jdbc.Driver");
            Connection con=DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/student","root","");

            Statement stmt=con.createStatement();
            ResultSet rs=stmt.executeQuery("select * from student_info");
            while(rs.next())
                System.out.println(rs.getInt(1)+"  "+rs.getString(2)+"  "+rs.getString(3));
            st_id = rs.getInt(1);
             st_name= rs.getString(2);
            st_status = rs.getString(3);

            producerFunc(st_id,st_name,st_status);

            con.close();
        }catch(Exception e){ System.out.println(e);}

    }

    public static void producerFunc(int st_id, String st_name, String st_status) throws ExecutionException, InterruptedException {
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
        String topic="input_topic";
        String value = st_id+" "+st_name+" "+st_status;
        String key = Integer.toString(st_id);

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
        }).get();
        producer.flush();
        //flush and close producer
        producer.close();
    }

}