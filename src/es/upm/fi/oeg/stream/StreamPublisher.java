package es.upm.fi.oeg.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLConnection;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;

import es.upm.fi.oeg.stream.Stream.FORMAT;


/*
 * This thread requests data the source and publish it to Kafka
 */
public class StreamPublisher implements Runnable {
	private Logger log = Logger.getLogger(this.getClass());
	
	private Stream stream;
	// RabbitMQ fields
	

	public StreamPublisher(Stream stream) {
		this.stream = stream;
	}
	
	@Override
	public void run() {
		try {
			String message = null;
			ProducerRecord<String, Object> record = null;
			// DEPRECATED - If the stream has form-based authentication credentials
//			if (stream.getUser() != null) {
//				// Form-based login - If the client is not authenticated
//				if (auth == null) {
//					auth = new FormBasedAuthentication(stream.getUrl().toString(), 
//							stream.getUrlLogin().toString(), stream.getUser(), stream.getPassword());
//					log.info("Form-based authentication.");
//					auth.authenticate();
//				}
//				message = auth.getData();
//				// TODO: support other authentication methods, e.g. Basic.
////				else if (!message.contains("j_username")) {					
////				}
//				record = new ProducerRecord<String, Object>(stream.getKafkaTopic(), message);
//				// Sends message to Kafka
//				StreamHandler.getInstance().getKafkaProducer().send(record);
//			}
			// If the stream comes from RabbitMQ
			if (stream.getRabbitMQQueue() != null) {
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(stream.getUrl().toString());
				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();
				channel.basicConsume(stream.getRabbitMQQueue(), new DefaultConsumer(channel) {
					// TODO: test basic consumer
				});
			}
			// DEFAULT: if the data comes from an API
			else {
				// Get data from the source API
				URLConnection connection = stream.getUrl().openConnection();
				BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				String line = null;
				// If the stream is a sequence of field-named tuples
				if (stream.getFormat().equalsIgnoreCase(FORMAT.CSV_DOCUMENT.toString())) {
					while ((line = br.readLine()) != null) {
						// Creates a Kafka record
						record = new ProducerRecord<String, Object>(stream.getKafkaTopic(), line);
						// Sends message to Kafka
						StreamHandler.getInstance().getKafkaProducer().send(record);
						System.out.println("INFO: Message sent!");
					}
				}
				else if (stream.getFormat().equalsIgnoreCase(FORMAT.CSV_LINE.toString())) {
					line = br.readLine();
					message = line;
					
					record = new ProducerRecord<String, Object>(stream.getKafkaTopic(), message);
					// Sends message to Kafka
					StreamHandler.getInstance().getKafkaProducer().send(record);
				}
				else if (stream.getFormat().equalsIgnoreCase(FORMAT.JSON.toString())) {
					// TODO: JSON support
				}
				else {
					// TODO: RDF support
				}
				
				
				// Closes the BufferedReader
				br.close();
			}
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
