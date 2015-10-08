package es.upm.fi.oeg.topology;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import es.upm.fi.oeg.bolt.LatencyObserverBolt;
import es.upm.fi.oeg.bolt.SensorCloudParserBolt;
import es.upm.fi.oeg.spout.FileSpout;

/*
 * Measures the amount of time to parse Sensor Cloud messages.
 * The result is a document, latencyResults.txt, that is stored at the Storm node where the LatencyObserverBolt is executed.
 * Each line of latencyResults.txt includes the latency in milliseconds of parsing a message, the timestamps of the processing and arrival, and the content of the message. 
 */
public class SensorCloudParsingLatencyTopology {
	
	public static void main(String[] args) throws Exception {
		
		TopologyBuilder builder = new TopologyBuilder();
		//builder.setSpout("sensorCloudSpout", new SensorCloudSpout());
		builder.setSpout("sensorCloudSpout", new FileSpout("/log1h.txt"));
		builder.setBolt("sensorCloudParser", new SensorCloudParserBolt(), 8).shuffleGrouping("sensorCloudSpout");
		builder.setBolt("latencyObserver", new LatencyObserverBolt("/tmp/latencyResults.txt")).shuffleGrouping("sensorCloudParser");
		
		// Topology general configuration
		Config config = new Config();
		config.setDebug(true);
		
		// To run the topology on the Storm cluster the call must include at least one argument, e.g. the topology name
		// Command executed on the Nimbus node: /opt/storm/bin/storm jar /shared/morph-streams-plus-plus-0.0.1-SNAPSHOT-jar-with-dependencies.jar... 
		// ...es.upm.fi.oeg.topology.SensorCloudParsingLatencyTopology parsing-latency-topology
		if (args != null && args.length > 0) {
			config.setNumWorkers(4);
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());

	    }
		else {
		  config.setMaxTaskParallelism(3);
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("parsing-latency-topology", config, builder.createTopology());

	      Thread.sleep(300000);

	      cluster.shutdown();
	    }
	}

}
