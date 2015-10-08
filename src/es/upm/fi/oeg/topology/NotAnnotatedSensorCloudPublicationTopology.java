package es.upm.fi.oeg.topology;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import es.upm.fi.oeg.bolt.KafkaPublisherBolt;
import es.upm.fi.oeg.bolt.LatencyObserverBolt;
import es.upm.fi.oeg.bolt.SensorCloudParserBolt;
import es.upm.fi.oeg.bolt.SweetAnnotationsBolt;
import es.upm.fi.oeg.spout.FileSpout;
import es.upm.fi.oeg.spout.SensorCloudSpout;

public class NotAnnotatedSensorCloudPublicationTopology {
	
	public static void main(String[] args) throws Exception {
				
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sensorCloudSpout", new FileSpout("/log1h.txt"));
		builder.setBolt("sensorCloudParser", new SensorCloudParserBolt(), 12).shuffleGrouping("sensorCloudSpout");
		builder.setBolt("kafkaPublisher", new KafkaPublisherBolt()).shuffleGrouping("sensorCloudParser");
		
		// Topology general configuration
		Config config = new Config();
		config.setDebug(true);
		config.setMaxSpoutPending(1024);
		
		// To run the topology on the Storm cluster the call must include at least one argument, e.g. the topology name
		// Command executed on the Nimbus node: /opt/storm/bin/storm jar /shared/morph-streams-plus-plus-0.0.1-SNAPSHOT-jar-with-dependencies.jar es.upm.fi.oeg.topology.NotAnnotatedSensorCloudPublicationTopology not-annotated-sensor-cloud-publication
		if (args != null && args.length > 0) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
	    }
		else {
	      config.setMaxTaskParallelism(3);
	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("not-annotated-sensor-cloud-publication", config, builder.createTopology());
	      Thread.sleep(20000);
	      cluster.shutdown();
	    }
	}

}
