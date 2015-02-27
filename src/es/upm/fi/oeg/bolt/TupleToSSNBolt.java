package es.upm.fi.oeg.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

/*
 * Bolt that converts input observation (field-named) tuples Semantic Sensor Network (SSN) model
 * The order of the input fields it is relevant to map to the output SSN properties
 */
public class TupleToSSNBolt extends BaseRichBolt{
	
	private static final long serialVersionUID = 8325660643098345088L;

	private OutputCollector outputCollector;
	
	public TupleToSSNBolt(String namespace, Fields outputFields) {
		
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector outputCollector) {
		this.outputCollector = outputCollector;
	}
	
	
	@Override
	public void execute(Tuple arg0) {
		// TODO Auto-generated method stub
		
	}

	

	/*
	 * Minimum amount of fields to define an ssn:Observation. 
	 * Leave empty or null the fields if the SSN property is not present in the data input.
	 * (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ssn:Observation", // 0001 -> http://myexample.org/obs/0001
				"ssn:observedProperty", 			   // sweet:Depth	
				"ssn:hasDataValue", 				   // 10 -> "10.5"^^xsd:float
				"ssn:observationSamplingTime", 	 	   // 2015-02-27T17:15:34Z -> "2015-02-27T17:15:34Z"^^xsd:datetime
				"geosparql:asWKT",					   // 27.7488 -18.0678 -> "<http://www.opengis.net/def/crs/EPSG/0/4258> POINT(27.7488 -18.0678)"^^geosparql:wktLiteral
				"ssn:observedBy",					   // seismometer-X65 -> http://myexample.org/sensor/seismometer-X65
				"ssn:featureOfInterest"));			   // earthquake-007 -> http://myexample.org/foi/earthquake-007
	}

}
