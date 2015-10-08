package es.upm.fi.oeg.topology;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import es.upm.fi.oeg.bolt.CSVToSSNGraphBolt;
import es.upm.fi.oeg.bolt.KafkaPublisherBolt;
import es.upm.fi.oeg.bolt.SensorCloudParserBolt;
import es.upm.fi.oeg.bolt.SweetAnnotationsBolt;
import es.upm.fi.oeg.spout.FileSpout;
import es.upm.fi.oeg.utils.SSNMapping;

public class SensorCloudRDFPublicationTopology {
	
	public static void main(String[] args) throws Exception {
				
		TopologyBuilder builder = new TopologyBuilder();
		//builder.setSpout("sensorCloudSpout", new SensorCloudSpout());
		builder.setSpout("sensorCloudSpout", new FileSpout("/log1h.txt"));
		builder.setBolt("sensorCloudParser", new SensorCloudParserBolt(), 12).shuffleGrouping("sensorCloudSpout");
		builder.setBolt("sweetAnnotator", new SweetAnnotationsBolt()).shuffleGrouping("sensorCloudParser");
		builder.setBolt("sensorCloudToSSN", new CSVToSSNGraphBolt("http://sensorcloud.linkeddata.es/")).shuffleGrouping("sweetAnnotator");
		builder.setBolt("kafkaPublisher", new KafkaPublisherBolt()).shuffleGrouping("sensorCloudToSSN");
		//builder.setBolt("latencyObserver", new LatencyObserverBolt("/tmp/sensorCloudPublication_latencyResults.txt")).shuffleGrouping("kafkaPublisher");
		
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
		// Command executed on the Nimbus node: /opt/storm/bin/storm jar /shared/morph-streams-plus-plus-0.0.1-SNAPSHOT-jar-with-dependencies.jar es.upm.fi.oeg.topology.SensorCloudRDFPublicationTopology sensor-cloud-RDF-publication
		if (args != null && args.length > 0) {
			config.setNumWorkers(1);
			StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
	    }
		else {
	      config.setMaxTaskParallelism(3);

	      LocalCluster cluster = new LocalCluster();
	      cluster.submitTopology("sensor-cloud-RDF-publication", config, builder.createTopology());

	      Thread.sleep(20000);

	      cluster.shutdown();
	    }
	}

}
