package es.upm.fi.oeg.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLConnection;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.upm.fi.oeg.utils.FormBasedAuthentication;


/*
 * This thread requests data the source and publish it to Kafka
 */
public class StreamPublisher implements Runnable {
	private Logger log = LoggerFactory.getLogger(StreamPublisher.class);
	private Stream stream;
	private FormBasedAuthentication auth;

	public StreamPublisher(Stream stream) {
		this.stream = stream;
	}
	
	@Override
	public void run() {
		try {
			String message;
			// If the stream has authentication credentials
			if (stream.getUser() != null) {
				// Form-based login - If the client is not authenticated
				if (auth == null) {
					auth = new FormBasedAuthentication(stream.getUrl().toString(), 
							stream.getUrlLogin().toString(), stream.getUser(), stream.getPassword());
					log.info("Form-based authentication.");
					auth.authenticate();
				}
				message = auth.getData();
				// TODO: support other authentication methods, e.g. Basic.
//				else if (!message.contains("j_username")) {					
//				}
			}
			// If the stream does not have authentication credentials
			else {
				// No need for authentication
				// Get data from the source
				URLConnection connection = stream.getUrl().openConnection();
				BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
				String line = null;
				StringBuilder strBuilder = new StringBuilder();
				while ((line = br.readLine()) != null) {
					strBuilder.append(line).append("\n");
				}
				message = strBuilder.toString();
				// Closes the BufferedReader
				br.close();
			}
			
			// Creates a Kafka record
			ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(stream.getTopic(), message);
			// Sends message to Kafka
			StreamHandler.getInstance().getKafkaProducer().send(record);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
