package es.upm.fi.oeg.bolt;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import es.upm.fi.oeg.spout.SensorCloudSpout;

/*
 * To execute this JUnit test, we run locally a Kafka cluster with 3 nodes, where the KafkaProducer can send messages.
 * One terminal launches Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties
 * A second terminal launches the lead Kafka broker with the configuration adjusted to auto-create topics: bin/kafka-server-start.sh config/server-auto.properties
 * A third terminal launches the second Kafka broker: bin/kafka-server-start.sh config/server-auto-1.properties
 * A fourth terminal launches the third Kafka broker: bin/kafka-server-start.sh config/server-auto-2.properties
 * Another terminal listens to the general channel with topic "all": bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic all
 * The last terminal listens to a specific channel, e.g. Temperature: bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic Temperature
 */
public class TestKafkaPublisherBolt {
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
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sensorCloudSpout", new SensorCloudSpout());
		builder.setBolt("sensorCloudParser", new SensorCloudParserBolt()).shuffleGrouping("sensorCloudSpout");
		builder.setBolt("sweetAnnotator", new SweetAnnotationsBolt()).shuffleGrouping("sensorCloudParser");
		builder.setBolt("kafkaPublisher", new KafkaPublisherBolt()).shuffleGrouping("sweetAnnotator");
		
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
