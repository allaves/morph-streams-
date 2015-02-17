package es.upm.fi.oeg.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLConnection;
import java.util.Collection;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import es.upm.fi.oeg.stream.Stream.FORMAT;
import scala.collection.mutable.DefaultEntry;
import storm.kafka.KafkaSpout;

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
	public boolean registerStream(Stream stream) {
		// Checks if the stream was already registered
		if (!streamRegistry.containsKey(stream.getId())) {
			// Registers the new stream
			streamRegistry.put(stream.getId(), stream);
			log.info("Stream registered with ID " + stream.getId());
			// Initiates the producer to start publishing data
			startDataPublishing(stream);
			
			return true;
		}
		log.error("ERROR! This stream was already registered: " + stream.getId());
		return false;
	}
	
	/*
	 * Initiates the data publishing to Kafka
	 */
	public void startDataPublishing(Stream stream) {
		// The publishing procedure varies depending on the data format
		if(stream.getFormat().equalsIgnoreCase(FORMAT.JSON.toString())) {
			// Creates a thread that reads from the JSON data source and sends messages to Kafka
			final Runnable jsonPublisher = new CSVPublisher(stream);
			// Schedule the thread to be executed regularly - at stream rate frecuency
			final ScheduledFuture<?> jsonPublisherTask = 
					scheduler.scheduleAtFixedRate(jsonPublisher, streamDelay, stream.getRate(), TimeUnit.MILLISECONDS);
			scheduledStreamsMap.put(stream.getId(), jsonPublisherTask);
		}
		else if(stream.getFormat().equalsIgnoreCase(FORMAT.CSV.toString())) {
			// Creates a thread that reads from the CSV data source and sends messages to Kafka
			final Runnable csvPublisher = new CSVPublisher(stream);
			// Schedule the thread to be executed regularly - at stream rate frecuency
			final ScheduledFuture<?> csvPublisherTask = 
					scheduler.scheduleAtFixedRate(csvPublisher, streamDelay, stream.getRate(), TimeUnit.MILLISECONDS);
			scheduledStreamsMap.put(stream.getId(), csvPublisherTask);
		}
		else if (stream.getFormat().equalsIgnoreCase(FORMAT.RDF.toString())) {
			// Do something
			
		}
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

	
	public Collection<Stream> getRegisteredStreams() {
		return streamRegistry.values();
	}
}
