package es.upm.fi.oeg.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Authenticator;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLConnection;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import es.upm.fi.oeg.stream.Stream.FORMAT;

public class TestStreamHandlerRabbitMQ {
	
	@Before
	public void setUp() {
		
	}
	
	@Test
	public void testParseSensorCloudMessage() {
		String message = "<sample time=\"2015-04-13T11:11:59.000\" value=\"100.0\" sensor=\"libelium.356893904.356893904-9247P-Sensiron-SHT75-air.relative-humidity\"/>";
		String[] messageArray = message.split(" ");
		String observationResultTime = messageArray[1].split("\"")[1];
		String value = messageArray[2].split("\"")[1];
		String[] path = messageArray[3].split("\"")[1].split("\\.");
		String network = path[0];
		String platform = path[1];
		String sensor = path[2];
		String phenomenon = path[3];		// Observed property
		String platformUrl = "http://www.sense-t.csiro.au/sensorcloud/v1/network/" + network + "/platform/" + platform;
		
		// Get coordinates from API
		String lat = null;
		String lon = null;
		String line = "";
		String objString = "";
		JSONObject jsonObj = null;
		try {
			URLConnection connection = new URL(platformUrl).openConnection();
			BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			while ((line = br.readLine()) != null) {
				objString += line;
			}
			JSONParser jsonParser = new JSONParser();
			jsonObj = (JSONObject) jsonParser.parse(objString);
			JSONObject platformObj = (JSONObject) jsonObj.get("platform");
			JSONObject locationObj = (JSONObject) platformObj.get("location");
			lon = (String) locationObj.get("longitude");
			lat = (String) locationObj.get("latitude");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Timestamp: " + observationResultTime);
		System.out.println("Value: " + value);
		System.out.println("Observed property: " + phenomenon);
		System.out.println("Network: " + network);
		System.out.println("Platform: " + platform);
		System.out.println("Sensor: " + sensor);
		System.out.println("Phenomenon: " + phenomenon);
		System.out.println("Latitude: " + lat);
		System.out.println("Longitude: " + lon);
	}

	

	//@Test
//	public void testRegisterStream() {
//		assertEquals("http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61--1000", 
//		StreamHandler.getInstance().registerStream(
//				"http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61", 
//				1000, FORMAT.CSV_DOCUMENT, "hsl"));
		
		
//	}

//	@Test
//	public void testGetRegisteredStreams() {
//		assertFalse(StreamHandler.getInstance().getRegisteredStreams().isEmpty());
//		assertEquals(StreamHandler.getInstance().getRegisteredStreams().size(), 2);
//	}
//	
//	@Test
//	public void testDeregisterStream() throws InterruptedException {
//		Thread.sleep(300000);
//		assertTrue(StreamHandler.getInstance().deregisterStream("https://esa.csiro.au/aus/alertHist?queryType=latest&listTweetIds=true--60000"));
//		assertFalse(StreamHandler.getInstance().deregisterStream("https://esa.csiro.au/aus/alertHist?queryType=latest&listTweetIds=true--60000"));
//		assertTrue(StreamHandler.getInstance().deregisterStream("http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61--1000"));
//		assertFalse(StreamHandler.getInstance().deregisterStream("http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61--1000"));
//	}


	

}
