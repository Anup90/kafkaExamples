package kafkaTest.kafkaTestProject;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Consumer {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
		 //Creating properties
	    String bootstrapServers = "localhost:9092";
	    String topic = "NewTopic";
	    String grp_id="thrid_app";
	    Properties properties = new Properties();
	    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
	    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
	    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, grp_id);
	    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    //creating producer
	    KafkaConsumer<String, String> consumer= new KafkaConsumer<>(properties);
	    //subscrbing
	    consumer.subscribe(Arrays.asList(topic));
	    //polling
	    while(true) {
	    	ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	    	for(ConsumerRecord<String, String> record: records)
	    	{
	    		logger.info("key: "+record.key()+"\n");
	    		logger.info("Partition "+ record.partition()+"\n"+"offset "+record.offset()+"\n");
	    		System.out.println("Value "+ record.value());
	    		
	    	}
	    	
	    }
	}

}
