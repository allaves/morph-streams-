package es.upm.fi.oeg.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLConnection;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.producer.ProducerRecord;

/*
 * This thread gets data from a CSV source and publish it to Kafka
 */
public class CSVPublisher implements Runnable {
	
	private Stream stream;

	public CSVPublisher(Stream stream) {
		this.stream = stream;
	}
	
	@Override
	public void run() {
		try {
			URLConnection connection = stream.getUrl().openConnection();
			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			StringBuilder strBuilder = new StringBuilder();
			String line = null;
			while ((line = br.readLine()) != null) {
				strBuilder.append(line).append("\n");
			}
			// Creates a Kafka record
			ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(stream.getKafkaTopic(), strBuilder.toString());
			// Sends message to Kafka
			StreamHandler.getInstance().getKafkaProducer().send(record);
			
			// Closes the BufferedReader
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}