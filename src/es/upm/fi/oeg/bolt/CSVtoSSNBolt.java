package es.upm.fi.oeg.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

/*
 * Bolt that converts input observation (field-named) tuples to RDF format
 * following the Semantic Sensor Network (SSN) model
 */
public class CSVtoSSNBolt extends BaseRichBolt{
	
	private static final long serialVersionUID = 8325660643098345088L;

	public CSVtoSSNBolt() {
		
	}

	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}

}
