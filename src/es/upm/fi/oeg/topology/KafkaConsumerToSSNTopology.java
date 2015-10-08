package es.upm.fi.oeg.topology;


import java.util.UUID;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import es.upm.fi.oeg.bolt.CSVToSSNGraphBolt;
import es.upm.fi.oeg.bolt.LatencyObserverBolt;
import es.upm.fi.oeg.utils.SSNMapping;
import es.upm.fi.oeg.utils.SSNTupleScheme;

/* 
 * Topology to consume Sensor Cloud data from Kafka and transform it to SSN graphs
 * Designed to be launched with Wirbelsturm (Vagrant)
 * https://github.com/miguno/wirbelsturm
 */
public class KafkaConsumerToSSNTopology {
	
	public static void main(String[] args) throws Exception {
		// Spout configuration
		BrokerHosts hosts = new ZkHosts("zookeeper1");
		// Subscribes to the topic where all messages are published
		String topic = "Temperature";
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new SSNTupleScheme());
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig), 2);
		builder.setBolt("sensorCloudToSSN", new CSVToSSNGraphBolt("http://sensorcloud.linkeddata.es/"), 2).shuffleGrouping("kafkaSpout");
		builder.setBolt("latencyObserver", new LatencyObserverBolt("/tmp/kafkaConsumerTopology_latencyResults.txt")).shuffleGrouping("sensorCloudToSSN");
		
		// Topology general configuration
		Config config = new Config();
		config.setDebug(true);
		config.setMaxSpoutPending(1024);
		
		// SSN Mapping configuration
		config.put(SSNMapping.MAPPING_OBSERVED_PROPERTY, "observedProperty");
		config.put(SSNMapping.MAPPING_DATA_VALUE, "value");
		config.put(SSNMapping.MAPPING_LATITUDE, "lat");
		config.put(SSNMapping.MAPPING_LONGITUDE, "lon");
		config.put(SSNMapping.MAPPING_OBSERVATION_RESULT_TIME, "observationSamplingTime");
		config.put(SSNMapping.MAPPING_OBSERVED_BY, "sensor");
		
		// To run the topology on the Storm cluster the call must include at least one argument, e.g. the topology name
		// Command executed on the Nimbus node: /opt/storm/bin/storm jar /shared/morph-streams-plus-plus-0.0.1-SNAPSHOT-jar-with-dependencies.jar...
		// ...es.upm.fi.oeg.topology.KafkaConsumerToSSNTopology kafka-consumer-ssn-topology
		if (args != null && args.length > 0) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
	    }
		else {
	      config.setMaxTaskParallelism(3);
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("kafka-consumer-ssn-topology", config, builder.createTopology());
	      Thread.sleep(20000);
	      cluster.shutdown();
	    }
	}

}
