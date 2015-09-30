package es.upm.fi.oeg.spout;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Before;
import org.junit.Test;

import es.upm.fi.oeg.bolt.AckerPrinterBolt;
import es.upm.fi.oeg.bolt.AckerWriterBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TestSensorCloudSpout {
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
		//builder.setBolt("printer", new AckerPrinterBolt()).shuffleGrouping("sensorCloudSpout");
		builder.setBolt("writer", new AckerWriterBolt("resources/log1h.txt")).shuffleGrouping("sensorCloudSpout");
		
		// Topology general configuration
		Config config = new Config();
		config.setDebug(true);
		config.put("host", "smg1-vic.it.csiro.au");
		config.put("user", user);
		config.put("password", password);
		config.put("queue", queue);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sensorCloud-test", config, builder.createTopology());
		
		Utils.sleep(3600000);
	    cluster.shutdown();
	}

}
