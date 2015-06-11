package es.upm.fi.oeg.bolt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import es.upm.fi.oeg.spout.SensorCloudSpout;
import es.upm.fi.oeg.utils.SSNMapping;

public class TestSensorCloudToSSNGraphBolt {
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
		builder.setBolt("sensorCloudToSSN", new CSVToSSNGraphBolt("http://data.sensorcloud.au/")).shuffleGrouping("sweetAnnotator");
		builder.setBolt("printer", new AckerPrinterBolt()).shuffleGrouping("sensorCloudToSSN");
		
		// Topology general configuration
		Config config = new Config();
		config.setDebug(false);
		config.setMessageTimeoutSecs(10);
		config.put("host", "smg1-vic.it.csiro.au");
		config.put("user", user);
		config.put("password", password);
		config.put("queue", queue);
		
		config.put(SSNMapping.OBSERVED_PROPERTY, "observedProperty");
		config.put(SSNMapping.MAPPING_DATA_VALUE, "value");
		config.put(SSNMapping.MAPPING_LATITUDE, "lat");
		config.put(SSNMapping.MAPPING_LONGITUDE, "lon");
		config.put(SSNMapping.MAPPING_OBSERVATION_RESULT_TIME, "observationSamplingTime");
		config.put(SSNMapping.MAPPING_OBSERVED_BY, "sensor");
		
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sensorCloud-test", config, builder.createTopology());
		
		Utils.sleep(40000);
	    cluster.shutdown();
	}

}
