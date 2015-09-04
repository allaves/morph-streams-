package es.upm.fi.oeg.bolt;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import org.junit.Before;
import org.junit.Test;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import es.upm.fi.oeg.spout.SensorCloudSpout;

public class TestSensorCloudParserBolt {
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
		// There are many ways for configuring the topology: 
		// - No parallelism
		// - Parallelism on the parsing:
		//   - 2 threads
		//   - 4 threads (better results in my laptop, local cluster)
		//   - ...
		// - Parallelism on the parsing + the writing
		//   - Shuffle grouping
		//   - Fields grouping: by network, by sensor, by phenomenon...
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sensorCloudSpout", new SensorCloudSpout());
		builder.setBolt("sensorCloudParser", new SensorCloudParserBolt(), 4).shuffleGrouping("sensorCloudSpout");
		builder.setBolt("printer", new AckerPrinterBolt()).shuffleGrouping("sensorCloudParser");
		
		// Topology general configuration
		Config config = new Config();
		config.setDebug(true);
		config.setMessageTimeoutSecs(10);
		config.put("host", "smg1-vic.it.csiro.au");
		config.put("user", user);
		config.put("password", password);
		config.put("queue", queue);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("sensorCloud-test", config, builder.createTopology());
		
		Utils.sleep(60000);
	    cluster.shutdown();
	}

}
