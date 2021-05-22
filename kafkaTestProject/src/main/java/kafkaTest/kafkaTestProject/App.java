package kafkaTest.kafkaTestProject;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.ILoggerFactory;
import org.slf4j.LoggerFactory;

public class App {
  public static void main(String[] args) {
    System.out.println("Hello World!");
    //Creating properties
    String bootstrapServers = "localhost:9092";
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
    //creating producer
    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    for(int i=0;i<=10;i++) {
    String topic ="NewTopic";
    String value = "onetwo$$"+Integer.toString(i);
    String key = "id"+Integer.toString(i);
    
    ILoggerFactory loggfact = LoggerFactory.getILoggerFactory();
    //creating producer record
    ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key,value);
    //Sending the data
    try {
		producer.send(record, new Callback() {
			
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				 org.slf4j.Logger logger = loggfact.getLogger(App.class.getName());
				//org.slf4j.Logger logger = loggerFactory.getLogger(App.class.getName());
				if(exception == null) {
					logger.info("Successfully recived details\n :");
					logger.info("Topic "+metadata.topic()+"\n");
					logger.info("Partion "+metadata.partition()+"\n");
					logger.info("offset "+metadata.offset()+"\n");
					logger.info("Timestamp "+metadata.timestamp()+"\n");
					
				}else {
					logger.error("Error cant send", exception);
				}
				
				
			}
		}).get();
	} catch (InterruptedException | ExecutionException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    }
    producer.flush();
    producer.close();
  }
}
