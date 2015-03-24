package es.upm.fi.oeg.bolt;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import javax.xml.datatype.DatatypeFactory;

import org.apache.jena.riot.stream.StreamManager;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

import es.upm.fi.oeg.stream.Stream;
import es.upm.fi.oeg.stream.Stream.FORMAT;
import es.upm.fi.oeg.stream.StreamHandler;
import es.upm.fi.oeg.utils.SSNMapping;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.KafkaUtils;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TestCSVToSSNGraphBolt {
	
	private Logger log = Logger.getLogger(this.getClass());

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
		builder.setBolt("printer", new AckerPrinterBolt()).shuffleGrouping("csvToSSN");
		
		// Topology general configuration
		Config config = new Config();
		//config.setMessageTimeoutSecs(5);
		
		// SSN mapping configuration
		Map<String, String> ssnConfig = new HashMap<String, String>();
				
		// The name of the CSV fields has to be submitted when the stream is registered in the system
		String[] fieldNames = {"vehicleId", "route", "lat", "lon", "bearing", "direction", "previousStop", "currentStop", "departure"};
		// Same happens with the dataType fields
		XSDDatatype[] fieldDataTypes = {XSDDatatype.XSDstring, XSDDatatype.XSDstring, XSDDatatype.XSDfloat, XSDDatatype.XSDfloat,
				XSDDatatype.XSDint, XSDDatatype.XSDstring, XSDDatatype.XSDstring, XSDDatatype.XSDstring, XSDDatatype.XSDstring};
		
		HashMap<String, String> ssnMapping = new HashMap<String, String>();
		// TODO: What happens when there is no observed property in the CSV? -> It should be submitted when the stream is registered in the system
		ssnConfig.put(SSNMapping.MAPPING_OBSERVED_PROPERTY, "vehicleDeparture");
		ssnConfig.put(SSNMapping.MAPPING_DATA_VALUE, "departure");
		ssnConfig.put(SSNMapping.MAPPING_LATITUDE, "lat");
		ssnConfig.put(SSNMapping.MAPPING_LONGITUDE, "lon");
		ssnConfig.put(SSNMapping.MAPPING_FEATURE_OF_INTEREST, "vehicleId");
		config.setEnvironment(ssnMapping);
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("hsl-test", config, builder.createTopology());
		
		StreamHandler.getInstance().registerCSVStream("http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61", 
				3000, FORMAT.CSV_DOCUMENT, "test", ';', fieldNames, fieldDataTypes, ssnMapping);
		
		Utils.sleep(30000);
	    cluster.shutdown();
	}

}
