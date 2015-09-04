package es.upm.fi.oeg.spout;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import es.upm.fi.oeg.bolt.AckerPrinterBolt;
import es.upm.fi.oeg.bolt.KafkaPublisherBolt;
import es.upm.fi.oeg.bolt.SensorCloudParserBolt;
import es.upm.fi.oeg.bolt.SweetAnnotationsBolt;
import es.upm.fi.oeg.spout.SensorCloudSpout;

/*
 * To execute this JUnit test, we run locally a Kafka cluster with 3 nodes, where the KafkaProducer can send messages.
 * One terminal launches Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
 * A second terminal launches the lead Kafka broker with the configuration adjusted to auto-create topics: bin/kafka-server-start.sh config/server-auto.properties
 * A third terminal launches the second Kafka broker: bin/kafka-server-start.sh config/server-auto-1.properties
 * A fourth terminal launches the third Kafka broker: bin/kafka-server-start.sh config/server-auto-2.properties
 * Then, we create the topics Rainfall and Temperature, e.g. bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 2 --partitions 2 --topic Rainfall
 * Another terminal publishes to the topic Temperature: bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Temperature
 * The last terminal publishes to the topic Rainfall: bin/kafka-console-producer.sh --broker-list localhost:9092 --topic Rainfall
 * Only the messages published to the topic Rainfall should appear in console
 */
public class TestKafkaSpoutConsumer {
	private String user;
	private String password;
	private String queue;

	@Before
	public void setUp() throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(new File("resources/credentials-sensor-cloud.txt")));
		// 1st line: user
		user = br.readLine();
		// 2nd line: password
		password = br.readLine();
		// 3rd line: queue
		queue = br.readLine();
		br.close();
	}

	@Test
	public void test() {
		// Zookeeper address
		BrokerHosts zkHost = new ZkHosts("localhost:2181");
		// Config example taken from https://github.com/apache/storm/tree/master/external/storm-kafka
		SpoutConfig spoutConfig = new SpoutConfig(zkHost, "Rainfall", "/Rainfall", "test");
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig));
		builder.setBolt("printer", new AckerPrinterBolt()).shuffleGrouping("kafkaSpout");
		
		// Topology general configuration
		Config config = new Config();
		//config.setDebug(true);
		config.setMaxTaskParallelism(4);
		//config.setMessageTimeoutSecs(10);
		config.setMaxSpoutPending(7000);
		config.put("host", "smg1-vic.it.csiro.au");
		config.put("user", user);
		config.put("password", password);
		config.put("queue", queue);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sensorCloud-test", config, builder.createTopology());
		
		Utils.sleep(40000);
	    cluster.shutdown();
	}

}
