package es.upm.fi.oeg.bolt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * This bolt initially reads a text file with mappings between Sensor Cloud phenomena
 * and SWEET concepts. Then, annotates each incoming observation tuple with the SWEET concept
 * as the observed property.
 */
public class SweetAnnotationsBolt extends BaseRichBolt {
	
	private static final String SWEET_NAMESPACE = "http://sweet.jpl.nasa.gov/2.3/sweetAll.owl/";
	private OutputCollector collector;
	private HashMap<String, String> sweetMappings;

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
		sweetMappings = new HashMap<String, String>();
		// Works for the IDE execution (testing), but not from in the cluster.
		//readAnnotationsFile("resources/annotations.csv");
		readAnnotationsFile("/annotations.csv");
	}

	
	private void readAnnotationsFile(String filePath) {
		try {
			// Works for the IDE execution (testing), but not from in the cluster.
			//BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
			InputStream is = getClass().getResourceAsStream(filePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			// Skips first line: Sensor Cloud phenomenon,SWEET concept
			String[] lineMapping;
			String line = br.readLine();
			while ((line = br.readLine()) != null) {
				lineMapping = line.split(",");
				sweetMappings.put(lineMapping[0], SWEET_NAMESPACE + lineMapping[1]);
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	

	@Override
	public void execute(Tuple tuple) {
		String phenomenon = tuple.getStringByField("phenomenon");
		System.out.println(phenomenon);
		
		String sweetConcept = null;
		if (sweetMappings.containsKey(phenomenon)) {
			sweetConcept = sweetMappings.get(phenomenon);
		}
		System.out.println("Annotation of " + phenomenon + ": " + sweetConcept);
		
		tuple.getValues().add(sweetConcept);
		//System.out.println(tuple);
		collector.emit(tuple.getValues());
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("observationResultTime", "observationSamplingTime", "value", "network", "platform", "sensor", "phenomenon", "lat", "lon", "observedProperty"));
	}

}
