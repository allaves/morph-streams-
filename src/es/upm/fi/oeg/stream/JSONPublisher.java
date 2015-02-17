package es.upm.fi.oeg.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLConnection;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.esotericsoftware.minlog.Log;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;


/*
 * This thread gets data from the JSON source and publish it to Kafka
 */
public class JSONPublisher implements Runnable {
	
	private Stream stream;

	public JSONPublisher(Stream stream) {
		this.stream = stream;
	}
	
	@Override
	public void run() {
		//try {
			// Get data from JSON source
			//URLConnection connection = stream.getUrl().openConnection();
//			Gson gson = new Gson();
//			InputStream is = stream.getUrl().openStream();
//			//connection.connect();
//			JsonReader reader = new JsonReader(new InputStreamReader(is, "UTF-8"));
//			
//			
//			StringBuilder strBuilder = new StringBuilder();
//			reader.beginArray();
//			while (reader.hasNext()) {
//				gson.fromJson(reader, String.class):
//				strBuilder.append(line).append("\n");
//			}
			
			
			// Creates a Kafka record
			//ProducerRecord<String, Object> record = new ProducerRecord<String, Object>(stream.getTopic(), strBuilder.toString());
			// Sends message to Kafka
			//StreamHandler.getInstance().getKafkaProducer().send(record);
			
			// Closes the BufferedReader
			//br.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
	}

}
