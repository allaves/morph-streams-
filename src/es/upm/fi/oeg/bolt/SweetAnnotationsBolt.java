package es.upm.fi.oeg.bolt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
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
	
	private static final String SWEET_NAMESPACE = "http://sweet.jpl.nasa.gov/2.3/sweetAll.owl";
	private OutputCollector collector;
	private HashMap<String, String> sweetMappings;

	@Override
	public void prepare(Map stormConf, TopologyContext context,	OutputCollector collector) {
		this.collector = collector;
		sweetMappings = new HashMap<String, String>();
		readAnnotationsFile("resources/annotations.csv");
	}

	private void readAnnotationsFile(String filePath) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(new File(filePath)));
			// Skips first line: Sensor Cloud phenomenon,SWEET concept
			String line = br.readLine();
			while ((line = br.readLine()) != null) {
				String[] lineMapping = line.split(",");
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
		String sweetConcept = sweetMappings.get(phenomenon);
		collector.emit(new Values(tuple.getValues().add(sweetConcept)));
		collector.ack(tuple);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("observationResultTime", "observationSamplingTime", "value", "network", "platform", "sensor", "phenomenon", "lat", "lon", "observedProperty"));
	}

}
