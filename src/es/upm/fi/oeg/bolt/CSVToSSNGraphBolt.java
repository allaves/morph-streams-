package es.upm.fi.oeg.bolt;

import java.util.Date;
import java.util.Locale;
import java.util.Map;

import javax.swing.text.DateFormatter;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.log4j.lf5.util.DateFormatManager;
import org.apache.xerces.xs.datatypes.XSDateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hp.hpl.jena.datatypes.DatatypeFormatException;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.NodeFactory;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
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
			// e.g. http://example.org/obs/0001/output ssn:hasValue "17.8"
			// TODO: infer the type of value to add a typed literal, e.g. xsd:float, instead of a plain literal
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId + "/output"), 
				ResourceFactory.createProperty(NS_SSN, "hasValue"), 
				ResourceFactory.createPlainLiteral(tuple.getStringByField((String)conf.get("ssn:hasDataValue")))).asTriple());
			// TODO: unit of measurement, e.g. qudt
		}
		else {
			log.error("Incomplete observation metadata - ssn:hasDataValue missing!");
		}
		
		// ssn:observationSamplingTime OR ssn:observationResultTime
		// TODO: format the input date according to xsd:dateTime
		if (conf.get("ssn:observationSamplingTime") != null) {
			// e.g. http://example.org/obs/0001 ssn:observationSamplingTime "2002-05-30T09:30:10+02:00"^^xsd:dateTime
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(NS_SSN, "observationSamplingTime"), 
				ResourceFactory.createTypedLiteral(tuple.getStringByField((String)conf.get("ssn:observationSamplingTime")), XSDDatatype.XSDdateTime)).asTriple());
		}
		else if (conf.get("ssn:observationResultTime") != null) {
			// e.g. http://example.org/obs/0001 ssn:observationResultTime "2002-05-30T09:30:10+02:00"^^xsd:dateTime
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(NS_SSN, "observationResultTime"), 
				ResourceFactory.createTypedLiteral(tuple.getStringByField((String)conf.get("ssn:observationResultTime")), XSDDatatype.XSDdateTime)).asTriple());
		}
		else {
			// If no timestamp is included in the observations, we add the system time.
			// e.g. http://example.org/obs/0001 ssn:observationResultTime "2002-05-30T09:30:10+02:00"^^xsd:dateTime
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(NS_SSN, "observationResultTime"), 
				ResourceFactory.createTypedLiteral(new DateFormatManager(Locale.getDefault(), "YYYY-DD-MMThh:mm:ssZ").format(new Date(System.currentTimeMillis())), 
						XSDDatatype.XSDdateTime)).asTriple());
		}
		
		// ssn:observedBy
		if (conf.get("ssn:observedBy") != null) {
			// e.g. http://example.org/obs/0001 ssn:observedBy http://example.org/sensor/0007
			String sensorId;
			if (!((String)conf.get("ssn:observedBy")).startsWith("http://")) {
				sensorId = namespace + "/sensor/" + (String)conf.get("ssn:observedBy");
			}
			else {
				sensorId = (String)conf.get("ssn:observedBy");
			}
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(NS_SSN, "observedBy"), 
				ResourceFactory.createResource(sensorId)).asTriple());
		} // No error message is added here because we do not consider the sensor metadata mandatory
		
		// geosparql:asWKT
		// We assume that the spatial location is a point given by two coordinates x and y in a String, i.e. "x y"
		// TODO: add support for lines and polygons.
		if (conf.get("geosparql:asWKT") != null) {
			String geom = namespace + "/geom/" + tuple.hashCode();
			// e.g. http://example.org/obs/0001 geosparql:hasGeometry http://example.org/geom/0002
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(NS_GEOSPARQL, "hasGeometry"), 
				ResourceFactory.createResource(geom)).asTriple());
			// e.g. http://example.org/geom/0002 geosparql:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4258> POINT(27.7488 -18.0678)"^^geosparql:wktLiteral
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(geom), 
				ResourceFactory.createProperty(NS_GEOSPARQL, "asWKT"), 
				ResourceFactory.createPlainLiteral("<" + crs + "> POINT(" + (String)conf.get("geosparql:asWKT") + ")")).asTriple());
			// TODO: create a wktLiteral instead of a plain literal. Problem: wktLiteral is not a RDFDataType.
		} // No error message is added here because we do not consider the sensor metadata mandatory
		
		// ssn:featureOfInterest
		if (conf.get("ssn:featureOfInterest") != null) {
			String foi;
			if (((String)conf.get("ssn:featureOfInterest")).startsWith("http://")) {
				foi = namespace + "/foi/" + tuple.hashCode();
			}
			else {
				foi = (String)conf.get("ssn:featureOfInterest");
			}
			// e.g. http://example.org/obs/0001 ssn:featureOfInterest http://example.org/foi/0235
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(NS_SSN, "featureOfInterest"), 
				ResourceFactory.createResource(foi)).asTriple());
		} // No error message is added here because we do not consider the sensor metadata mandatory
		
		collector.emit(new Values(observationId, System.currentTimeMillis(), graph));
	}


	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("name", "timestamp", "graph"));
	}
	

}
