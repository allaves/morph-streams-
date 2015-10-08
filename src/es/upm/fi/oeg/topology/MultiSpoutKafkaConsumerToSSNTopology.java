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
 * Topology to consume not annotated Sensor Cloud data from multiple Kafka topics and transform it to SSN graphs
 * Designed to be launched with Wirbelsturm (Vagrant)
 * https://github.com/miguno/wirbelsturm
 */
public class MultiSpoutKafkaConsumerToSSNTopology {
	
	public static void main(String[] args) throws Exception {
		// Spout configuration
		BrokerHosts hosts = new ZkHosts("zookeeper1");
		// Subscribes to the topics where temperature-related messages are published:
		// "Temperature"
		// "temperature"
		// "air_temp"
		// "air-temperature"
		// "temperature_deg_c"
		// "average-air-temperature"
		// "logger-temperature"
		String topic = "Temperature";
		SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new SSNTupleScheme());
		KafkaSpout spoutTemperature = new KafkaSpout(spoutConfig);
		
		topic = "temperature";
		spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new SSNTupleScheme());
		KafkaSpout spouttemperature = new KafkaSpout(spoutConfig);
		
		topic = "air_temp";
		spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new SSNTupleScheme());
		KafkaSpout spoutAirTemp = new KafkaSpout(spoutConfig);
		
		topic = "air-temperature";
		spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new SSNTupleScheme());
		KafkaSpout spoutAirTemperature = new KafkaSpout(spoutConfig);
		
		topic = "temperature_deg_c";
		spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new SSNTupleScheme());
		KafkaSpout spoutTemperatureDegC = new KafkaSpout(spoutConfig);
		
		topic = "average-air-temperature";
		spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new SSNTupleScheme());
		KafkaSpout spoutAverageAirTemperature = new KafkaSpout(spoutConfig);
		
		topic = "logger-temperature";
		spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new SSNTupleScheme());
		KafkaSpout spoutLoggerTemperature = new KafkaSpout(spoutConfig);
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout-Temperature", spoutTemperature, 2);
		builder.setSpout("spout-temperature", spouttemperature, 2);
		builder.setSpout("spout-air_temp", spoutAirTemp, 2);
		builder.setSpout("spout-air-temperature", spoutAirTemperature, 2);
		builder.setSpout("spout-temperature_deg_c", spoutTemperatureDegC, 2);
		builder.setSpout("spout-average-air-temperature", spoutAverageAirTemperature, 2);
		builder.setSpout("spout-logger-temperature", spoutLoggerTemperature, 2);
		builder.setBolt("sensorCloudToSSN", new CSVToSSNGraphBolt("http://sensorcloud.linkeddata.es/"), 12)
			.shuffleGrouping("spout-Temperature").shuffleGrouping("spout-temperature").shuffleGrouping("spout-air_temp")
			.shuffleGrouping("spout-air-temperature").shuffleGrouping("spout-temperature_deg_c").shuffleGrouping("spout-average-air-temperature")
			.shuffleGrouping("spout-logger-temperature");
		builder.setBolt("latencyObserver", new LatencyObserverBolt("/tmp/kafkaConsumerTopology_latencyResults.txt")).shuffleGrouping("sensorCloudToSSN");
		
		// Topology general configuration
		Config config = new Config();
		config.setDebug(true);
		config.setMaxSpoutPending(1024);
		
		// SSN Mapping configuration
		config.put(SSNMapping.MAPPING_OBSERVED_PROPERTY, "phenomenon");	// Without annotation, the mapping points to the field "phenomenon" instead of to "observedProperty".
		config.put(SSNMapping.MAPPING_DATA_VALUE, "value");
		config.put(SSNMapping.MAPPING_LATITUDE, "lat");
		config.put(SSNMapping.MAPPING_LONGITUDE, "lon");
		config.put(SSNMapping.MAPPING_OBSERVATION_RESULT_TIME, "observationSamplingTime");
		config.put(SSNMapping.MAPPING_OBSERVED_BY, "sensor");
		
		// To run the topology on the Storm cluster the call must include at least one argument, e.g. the topology name
		// Command executed on the Nimbus node: /opt/storm/bin/storm jar /shared/morph-streams-plus-plus-0.0.1-SNAPSHOT-jar-with-dependencies.jar es.upm.fi.oeg.topology.MultiSpoutKafkaConsumerToSSNTopology multi-spout-kafka-consumer
		if (args != null && args.length > 0) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
	    }
		else {
	      config.setMaxTaskParallelism(3);
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("multi-spout-kafka-consumer", config, builder.createTopology());
	      Thread.sleep(20000);
	      cluster.shutdown();
	    }
	}
}
