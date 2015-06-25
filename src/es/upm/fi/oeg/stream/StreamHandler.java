package es.upm.fi.oeg.stream;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import es.upm.fi.oeg.stream.Stream.FORMAT;
import es.upm.fi.oeg.utils.SSNMapping;

/*
 * Singleton
 */
public class StreamHandler {
	
	private HashMap<String, Stream> streamRegistry;
	private KafkaProducer<String, Object> kafkaProducer;	// Let's start with a unique async Kafka producer
	private HashMap<String, ScheduledFuture<?>> scheduledStreamsMap;	// Maps stream ids to the scheduled task that sends data regularly
	private ScheduledExecutorService scheduler;
	private long streamDelay;
	
	private Logger log = LoggerFactory.getLogger(StreamHandler.class);
	
	/*
	 * Singleton stuff starts
	 */
	private static StreamHandler instance;
	
	private StreamHandler() {
		streamRegistry = new HashMap<String, Stream>();
		scheduledStreamsMap = new HashMap<String, ScheduledFuture<?>>();
		kafkaProducer = createKafkaProducer();
		// Pool of workers with 4 threads to be scheduled
		scheduler = Executors.newScheduledThreadPool(4);
		streamDelay = 0;
	}
	
	public static StreamHandler getInstance() {
		if (instance == null) {
			createInstance();
		}
		return instance;
	}
	
	private static void createInstance() {
		if (instance == null) {
			synchronized(StreamHandler.class) {
				if (instance == null) {
					instance = new StreamHandler();
				}
			}
		}
	}
	
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}
	/*
	 * Singleton stuff ends
	 */
	
	
	/*
	 * Access to the Kafka producer
	 */
	public KafkaProducer<String, Object> getKafkaProducer() {
		return kafkaProducer;
	}
	
	/*
	 * Defines the configuration of the Kafka producer
	 * Properties are documented at http://kafka.apache.org/documentation.html#newproducerconfigs
	 */
	private KafkaProducer<String, Object> createKafkaProducer() {
		// Defines minimal properties of the Kafka producer (will be defined by the client in a future?)
		Properties props = new Properties();
		props.put("producer.type", "async");
		props.put("request.required.acks", "1");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("bootstrap.servers", "localhost:9092");
		// Instantiates the producer
		return new KafkaProducer<String, Object>(props);
	}

	/*
	 * Registers a stream (if it was not in the registry) and starts publishing data to Kafka
	 */
	private boolean registerStream(Stream stream) {
		// Checks if the stream was already registered
		if (!streamRegistry.containsKey(stream.getId())) {
			// Registers the new stream
			streamRegistry.put(stream.getId(), stream);
			log.info("Stream registered with ID: " + stream.getId());
			// Initiates the producer to start publishing data
			//startDataPublishing(stream);
			return true;
		}
		else {
			log.error("ERROR! This stream was already registered: " + stream.getId());
			return false;
		}
	}
	
	
	/*
	 * Registers a stream (if it was not in the registry) and starts publishing data to Kafka
	 */
	public String registerCSVStream(String url, int millisecondsRate, FORMAT format, String kafkaTopic, char delimiter, 
			String[] fieldNames, XSDDatatype[] fieldDataTypes, Map<String, String> ssnMapping) {
		Stream stream = null;
		try {
			stream = new CSVStream(new URL(url), millisecondsRate, format, kafkaTopic, delimiter, fieldNames, fieldDataTypes, ssnMapping);
		} catch (MalformedURLException e) {
			log.error(e.getMessage());
			return null;
		}
		if (registerStream(stream)) {
			return stream.getId();
		}
		return null;
	}
	
	
	/*
	 * Registers a stream that requires form-based login (if it was not in the registry) and starts publishing data to Kafka
	 */
	public String registerStreamWithFormBasedLogin(String url, int millisecondsRate, FORMAT format, String kafkaTopic, 
			String user, String password, String urlLogin) {
		Stream stream = null;
		try {
			stream = new Stream(new URL(url), millisecondsRate, format, kafkaTopic, user, password, new URL(urlLogin));
		} catch (MalformedURLException e) {
			log.error(e.getMessage());
			return null;
		}
		if (registerStream(stream)) {
			return stream.getId();
		}
		return null;
	}
	
	
	/*
	 * Registers a stream from RabbitMQ (if it was not in the registry) and starts publishing data to Kafka
	 */
	public String registerStreamRabbitMQ(String url, int millisecondsRate, FORMAT format, String kafkaTopic, String user, String password, String rabbitMQExchange, SSNMapping ssnMapping) {
		Stream stream = null;
		try {
			stream = new Stream(new URL(url), millisecondsRate, format, kafkaTopic, user, password, rabbitMQExchange, ssnMapping);
		} catch (MalformedURLException e) {
			log.error(e.getMessage());
			return null;
		}
		if (registerStream(stream)) {
			return stream.getId();
		}
		return null;
	}
	
	
	/*
	 * Initiates the data publishing to Kafka
	 */
	public void startDataPublishing(Stream stream) {
		// Creates a thread that reads from the data source and sends messages to Kafka
		final Runnable streamPublisher = new StreamPublisher(stream);
		// Schedule the thread to be executed regularly - at stream rate frecuency
		final ScheduledFuture<?> streamPublisherTask = 
				scheduler.scheduleAtFixedRate(streamPublisher, streamDelay, stream.getMillisecondsRate(), TimeUnit.MILLISECONDS);
		scheduledStreamsMap.put(stream.getId(), streamPublisherTask);
	}

	
	/*
	 * Stops publishing data from the stream and removes it from the registry
	 */
	public boolean deregisterStream(String streamId) {
		if (streamRegistry.containsKey(streamId) && scheduledStreamsMap.containsKey(streamId)) {
			// Stops the scheduled task of publishing data
			scheduledStreamsMap.get(streamId).cancel(true);
			scheduledStreamsMap.remove(streamId);
			// Removes the stream from the registry
			streamRegistry.remove(streamId);
			log.info(streamId + " deregistered succesfully!");
			return true;
		}
		log.error("ERROR! " + streamId + " stream could not be deregistered.");
		return false;
	}
	
	
	/*
	 * Stops the publisher thread, closes the producer and removes it from the producer registry
	 */
	public void stopsKafkaProducer() {
		kafkaProducer.close();
		log.info("Kafka producer stopped.");
	}

	
	/*
	 * Returns the ids of the registered streams
	 */
	public Collection<String> getRegisteredStreams() {
		return streamRegistry.keySet();
	}

	/*
	 * Returns a Stream given its identifier
	 */
	public Stream getStream(String streamUri) {
		return streamRegistry.get(streamUri);
	}

}
