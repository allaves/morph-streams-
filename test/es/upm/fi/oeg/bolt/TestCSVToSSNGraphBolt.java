package es.upm.fi.oeg.bolt;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.KafkaUtils;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;

public class TestCSVToSSNGraphBolt {

	@Before
	public void setUp() throws Exception {
	}

	@Test
	public void test() {
		TopologyBuilder builder = new TopologyBuilder();
		// Documentation related to KafkaSpout at https://github.com/apache/storm/tree/master/external/storm-kafka
		BrokerHosts host = new ZkHosts("localhost:2181");
		String topic = "test";
		// The zkRoot is where Storm keeps metadata about what is consumed, i.e. localhost:2181/kafkastorm
		String zkRoot = "/kafkastorm/" + topic;
		String spoutId = "kafkaSpout";
		// Kafka spout configuration
		SpoutConfig spoutConfig = new SpoutConfig(host, topic, zkRoot, spoutId);
		spoutConfig.stateUpdateIntervalMs = 2000;
		builder.setSpout("kafkaSpout", new KafkaSpout(spoutConfig));
		String namespace = "http://morph-streams-plus-plus/test";
		CSVToSSNGraphBolt csvToSSNBolt = new CSVToSSNGraphBolt(namespace);
		builder.setBolt("csvToSSN", csvToSSNBolt).shuffleGrouping("kafkaSpout");
		builder.setBolt("printer", new PrinterBolt()).shuffleGrouping("csvToSSN");
		
		// Topology general configuration
		Config config = new Config();
		config.setMessageTimeoutSecs(5);
		// SSN mapping configuration
		Map<String, String> ssnConfig = new HashMap<String, String>();
		
		
		// TODO: The name of the CSV fields should be submitted when the stream is registered in the system
		//ssnConfig.put(csvToSSNBolt.MAPPING_OBSERVATION_ID, vehicleId);
		// TODO: What happens when there is no observed property in the CSV? -> It should be submitted when the stream is registered in the system
		ssnConfig.put(csvToSSNBolt.MAPPING_OBSERVED_PROPERTY, ...);
		ssnConfig.put(csvToSSNBolt.MAPPING_DATA_VALUE, ...);
		// ...To complete
		
		
		
		config.setEnvironment(ssnConfig);
	}

}
