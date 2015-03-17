package es.upm.fi.oeg.bolt;

import java.util.Map;

import org.apache.jena.riot.RDFDataMgr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
 * Bolt that converts input observations in CSV (field-named tuples) to Semantic Sensor Network (SSN) graphs.
 * We assume that the user who submits the topology passes the field names of the CSV in key-value pairs
 * 	on a backtype.storm.Config object at cluster.submitTopology(String, Config, StormTopology). 
 * We recommend the following properties for a minimal SSN-compliant observation: 
 * 	"observationId", "ssn:observedProperty", "ssn:hasDataValue", "ssn:observationSamplingTime"/"ssn:observationResultTime", and "geosparql:asWKT".
 * More properties can be added such as: "ssn:featureOfInterest", "ssn:observedBy",...
 */
public class CSVToSSNGraphBolt extends BaseRichBolt{
	
	private static final long serialVersionUID = 8325660643098345088L;

	private static String NS_SSN = "http://purl.oclc.org/NET/ssnx/ssn#";
	private static String NS_GEOSPARQL = "http://www.opengis.net/ont/geosparql#";
	private static String NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	private static String NS_XSD = "http://www.w3.org/2001/XMLSchema#";
	
	private Logger log = LoggerFactory.getLogger(this.getClass());
	private OutputCollector collector;
	private String namespace;
	private String crs;
	private Map conf;
	
	
	/*
	 * The constructor of the bolt can receive some parameters that do not vary for the sensor data stream:
	 * namespace, observed property, and coordinate reference system.
	 */
	public CSVToSSNGraphBolt(String namespace, String crs) {
		if (namespace.endsWith("/") || namespace.endsWith("#")) {
			this.namespace = namespace.substring(0, -1);
		}
		else {
			this.namespace = namespace;
		}
		this.crs = crs;
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.conf = conf;
	}
	
	
	@Override
	public void execute(Tuple tuple) {
		Graph graph = Factory.createDefaultGraph();
		// ssn:Observation
		String observationId = namespace + "/obs/";
		if (conf.get("observationId") != null) {
			observationId += tuple.getStringByField((String)conf.get("observationId"));
		}
		else {
			observationId += tuple.hashCode();
		}
		// e.g. http://example.org/obs/0001 rdf:type ssn:Observation
		graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(NS_RDF, "type"), 
				ResourceFactory.createResource(NS_SSN + "Observation")).asTriple());
		
		// ssn:observedProperty
		if (conf.get("ssn:observedProperty") != null) {
			// e.g. http://example.org/obs/0001 ssn:observedProperty sweet:AirTemperature
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
					ResourceFactory.createProperty(NS_SSN, "observedProperty"), 
					ResourceFactory.createResource(tuple.getStringByField((String)conf.get("ssn:observedProperty")))).asTriple());
		}
		else {
			log.error("Incomplete observation metadata - ssn:observedProperty missing!");
		}
		
		// ssn:hasDataValue
		if (conf.get("ssn:hasDataValue") != null) {
			// e.g. http://example.org/obs/0001 ssn:observationResult http://example.org/obs/0001/output
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
					ResourceFactory.createProperty(NS_SSN, "observationResult"), 
					ResourceFactory.createResource(observationId + "/output")).asTriple());
			// e.g. http://example.org/obs/0001/output ssn:hasValue "17.8"^^xsd:float
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId + "/output"), 
					ResourceFactory.createProperty(NS_SSN, "hasValue"), 
					ResourceFactory.createPlainLiteral(tuple.getStringByField((String)conf.get("ssn:hasDataValue")))).asTriple());
		}
		else {
			log.error("Incomplete observation metadata - ssn:hasDataValue missing!");
		}
		
		// ssn:observedBy
				
		collector.emit(new Values(observationId, System.currentTimeMillis(), graph));
	}


	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("name", "timestamp", "graph"));
	}
	

}
