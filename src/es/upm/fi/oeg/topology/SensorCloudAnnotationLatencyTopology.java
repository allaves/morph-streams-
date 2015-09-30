package es.upm.fi.oeg.topology;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.KillOptions;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import es.upm.fi.oeg.bolt.LatencyObserverBolt;
import es.upm.fi.oeg.bolt.SensorCloudParserBolt;
import es.upm.fi.oeg.bolt.SweetAnnotationsBolt;
import es.upm.fi.oeg.spout.SensorCloudSpout;

/*
 * Measures the amount of time to parse and annotate Sensor Cloud messages.
 * The result is a document, latencyResults.txt, that is stored at the Storm node where the LatencyObserverBolt is executed.
 * Each line of latencyResults.txt includes the latency in milliseconds of parsing a message, the timestamps of the processing and arrival, and the content of the message. 
 */
public class SensorCloudAnnotationLatencyTopology {
	
	public static void main(String[] args) throws Exception {
		// File path in the jar
		InputStream is = SensorCloudAnnotationLatencyTopology.class.getResourceAsStream("/credentials-sensor-cloud.txt");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		//BufferedReader br = new BufferedReader(new FileReader(new File("credentials-sensor-cloud.txt")));
		// 1st line: user
		String user = br.readLine();
		// 2nd line: password
		String password = br.readLine();
		// 3rd line: queue
		String queue = br.readLine();
		br.close();
		
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sensorCloudSpout", new SensorCloudSpout());
		builder.setBolt("sensorCloudParser", new SensorCloudParserBolt()).shuffleGrouping("sensorCloudSpout");
		builder.setBolt("sweetAnnotator", new SweetAnnotationsBolt()).shuffleGrouping("sensorCloudParser");
		builder.setBolt("latencyObserver", new LatencyObserverBolt()).shuffleGrouping("sweetAnnotator");
		
		// Topology general configuration
		Config config = new Config();
		config.setDebug(true);
		//config.setMaxTaskParallelism(8);
		//config.setMessageTimeoutSecs(10);
		config.setMaxSpoutPending(1024);
		config.put("host", "smg1-vic.it.csiro.au");
		config.put("user", user);
		config.put("password", password);
		config.put("queue", queue);
		
		// Copied from Storm starter WordCountTopology
		// https://github.com/apache/storm/blob/master/examples/storm-starter/src/jvm/storm/starter/WordCountTopology.java
		// To run the topology on the Storm cluster the call must include at least one argument, e.g. the topology name
		if (args != null && args.length > 0) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
	    }
		else {
		  config.setMaxTaskParallelism(3);
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("annotation-latency-topology", config, builder.createTopology());

	      Thread.sleep(20000);

	      cluster.shutdown();
	    }
	}

}
