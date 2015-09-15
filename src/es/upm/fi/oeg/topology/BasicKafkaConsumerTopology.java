package es.upm.fi.oeg.topology;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import es.upm.fi.oeg.bolt.AckerPrinterBolt;
import es.upm.fi.oeg.bolt.KafkaPublisherBolt;
import es.upm.fi.oeg.bolt.LatencyObserverBolt;
import es.upm.fi.oeg.bolt.SensorCloudParserBolt;
import es.upm.fi.oeg.bolt.SweetAnnotationsBolt;
import es.upm.fi.oeg.spout.SensorCloudSpout;

/* 
 * Topology to consume Sensor Cloud data from Kafka
 * Designed to be launched with Wirbelsturm (Vagrant)
 * https://github.com/miguno/wirbelsturm
 */
public class BasicKafkaConsumerTopology {
	
	public static void main(String[] args) throws Exception {
		// Spout configuration
		BrokerHosts hosts = new ZkHosts("zookeeper1");
		// Subscribes to the topic where all messages are published
		String topic = "all";
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig));
		//builder.setBolt("sensorCloudParser", new SensorCloudParserBolt()).shuffleGrouping("sensorCloudSpout");
		//builder.setBolt("sweetAnnotator", new SweetAnnotationsBolt()).shuffleGrouping("sensorCloudParser");
		//builder.setBolt("acker", new AckerPrinterBolt()).shuffleGrouping("kafkaSpout");
		builder.setBolt("latencyObserver", new LatencyObserverBolt()).shuffleGrouping("kafkaSpout");
		
		// Topology general configuration
		Config config = new Config();
		config.setDebug(true);
		//config.setMaxTaskParallelism(4);
		//config.setMessageTimeoutSecs(10);
		config.setMaxSpoutPending(7000);
		
		// Copied from Storm starter WordCountTopology
		// https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/storm/starter/WordCountTopology.java
		// To run the topology on the Storm cluster the call must include at least one argument, e.g. the topology name
		// Command executed on the Nimbus node: 
		if (args != null && args.length > 0) {
			config.setNumWorkers(3);
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
	    }
		else {
	      config.setMaxTaskParallelism(3);

	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("kafka-consumer-topology", config, builder.createTopology());

	      Thread.sleep(20000);

	      cluster.shutdown();
	    }
	}

}
