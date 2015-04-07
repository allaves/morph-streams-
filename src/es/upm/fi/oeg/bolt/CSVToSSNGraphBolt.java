package es.upm.fi.oeg.bolt;

import java.util.Date;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.lf5.util.DateFormatManager;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import es.upm.fi.oeg.utils.SSNMapping;

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
	
	//private Logger log = Logger.getLogger(this.getClass());
	private OutputCollector collector;
	private String namespace;
	private Map conf;
	private Graph graph;
	
	
	/*
	 * The constructor of the bolt can receive some parameters that do not vary for the sensor data stream:
	 * namespace, observed property, and coordinate reference system.
	 */
	public CSVToSSNGraphBolt(String namespace) {
		if (namespace.endsWith("/") || namespace.endsWith("#")) {
			this.namespace = namespace.substring(0, -1);
		}
		else {
			this.namespace = namespace;
		}
	}

	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.conf = conf;
		this.graph = Factory.createDefaultGraph();
	}
	
	
	@Override
	public void execute(Tuple tuple) {
		// ssn:Observation
		String observationId = namespace + "/obs/";
		if (conf.get(SSNMapping.MAPPING_OBSERVATION_ID) != null) {
			observationId += tuple.getStringByField((String)conf.get(SSNMapping.MAPPING_OBSERVATION_ID));
		}
		else {
			observationId += tuple.hashCode();
		}
		// e.g. http://example.org/obs/0001 rdf:type ssn:Observation
		graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
			ResourceFactory.createProperty(SSNMapping.NS_RDF, "type"), 
			ResourceFactory.createResource(SSNMapping.NS_SSN + "Observation")).asTriple());
		
		// ssn:observedProperty
		String observedProperty = null;
		if (conf.get(SSNMapping.OBSERVED_PROPERTY) != null) {
			if (!((String)conf.get(SSNMapping.MAPPING_OBSERVED_PROPERTY)).startsWith("http://")) {
				// http://example.org/observedProperty/vehiclePosition
				observedProperty = namespace + "/observedProperty/" + (String)conf.get(SSNMapping.MAPPING_OBSERVED_PROPERTY);
			}
			else {
				observedProperty = (String)conf.get(SSNMapping.MAPPING_OBSERVED_PROPERTY);
			}
		}
		else if (conf.get(SSNMapping.OBSERVED_PROPERTY) != null) {
			observedProperty = tuple.getStringByField((String)conf.get(SSNMapping.OBSERVED_PROPERTY));
		}
		if (observedProperty != null) {
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
					ResourceFactory.createProperty(SSNMapping.NS_SSN, "observedProperty"), 
					ResourceFactory.createResource(observedProperty)).asTriple());
		}
		else {
			System.out.println("ERROR: Incomplete observation metadata - ssn:observedProperty missing! - " + tuple);
		}
		
		// ssn:hasDataValue
		if (conf.get(SSNMapping.MAPPING_DATA_VALUE) != null) {
			// e.g. http://example.org/obs/0001 ssn:observationResult http://example.org/obs/0001/output
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(SSNMapping.NS_SSN, "observationResult"), 
				ResourceFactory.createResource(observationId + "/output")).asTriple());
			// e.g. http://example.org/obs/0001/output ssn:hasValue "17.8"
			// TODO: infer the type of value to add a typed literal, e.g. xsd:float, instead of a plain literal
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId + "/output"), 
				ResourceFactory.createProperty(SSNMapping.NS_SSN, "hasValue"), 
				ResourceFactory.createPlainLiteral(tuple.getStringByField((String)conf.get(SSNMapping.MAPPING_DATA_VALUE)))).asTriple());
			// TODO: unit of measurement, e.g. qudt
		}
		else {
			System.out.println("ERROR: Incomplete observation metadata - ssn:hasDataValue missing! - " + tuple);
		}
		
		// ssn:observationSamplingTime OR ssn:observationResultTime
		// TODO: format the input date according to xsd:dateTime
		if (conf.get("ssn:observationSamplingTime") != null) {
			// e.g. http://example.org/obs/0001 ssn:observationSamplingTime "2002-05-30T09:30:10+02:00"^^xsd:dateTime
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(SSNMapping.NS_SSN, "observationSamplingTime"), 
				ResourceFactory.createTypedLiteral(tuple.getStringByField((String)conf.get("ssn:observationSamplingTime")), XSDDatatype.XSDdateTime)).asTriple());
		}
		else if (conf.get("ssn:observationResultTime") != null) {
			// e.g. http://example.org/obs/0001 ssn:observationResultTime "2002-05-30T09:30:10+02:00"^^xsd:dateTime
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(SSNMapping.NS_SSN, "observationResultTime"), 
				ResourceFactory.createTypedLiteral(tuple.getStringByField((String)conf.get("ssn:observationResultTime")), XSDDatatype.XSDdateTime)).asTriple());
		}
		else {
			// If no timestamp is included in the observations, we add the system time.
			// e.g. http://example.org/obs/0001 ssn:observationResultTime "2002-05-30T09:30:10+02:00"^^xsd:dateTime
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(SSNMapping.NS_SSN, "observationResultTime"), 
				ResourceFactory.createTypedLiteral(new DateFormatManager(Locale.getDefault(), "YYYY-DD-MMThh:mm:ssZ").format(new Date(System.currentTimeMillis())), XSDDatatype.XSDdateTime)).asTriple());
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
				ResourceFactory.createProperty(SSNMapping.NS_SSN, "observedBy"), 
				ResourceFactory.createResource(sensorId)).asTriple());
		} // No error message is added here because we do not consider the sensor metadata mandatory
		
		// geosparql:asWKT
		// We assume that the spatial location is a point given by two coordinates x and y in a String, i.e. "x y"
		// TODO: add support for lines and polygons.
		String latLon = null;
		if (conf.get("geosparql:asWKT") != null) {
			latLon = (String)conf.get("geosparql:asWKT");
		}
		else if ((conf.get("lat") != null) && (conf.get("lon") != null)) {
			latLon = conf.get("lat") + " " + conf.get("lon");
		}
		if (latLon != null) {
			String crs = (String)conf.get("crs");
			if (crs == null) {
				// If no crs is provided, we assume that the data follows the global default EPSG:4326.
				crs = "http://www.opengis.net/def/crs/EPSG/0/4326";
			}
			String geom = namespace + "/geom/" + tuple.hashCode();
			// e.g. http://example.org/obs/0001 geosparql:hasGeometry http://example.org/geom/0002
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(SSNMapping.NS_GEOSPARQL, "hasGeometry"), 
				ResourceFactory.createResource(geom)).asTriple());
			// e.g. http://example.org/geom/0002 geosparql:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4258> POINT(27.7488 -18.0678)"^^geosparql:wktLiteral
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(geom), 
				ResourceFactory.createProperty(SSNMapping.NS_GEOSPARQL, "asWKT"), 
				ResourceFactory.createPlainLiteral("<" + crs + "> POINT(" + latLon + ")")).asTriple());
			// TODO: create a wktLiteral instead of a plain literal. Problem: wktLiteral is not a RDFDataType.
		} // No error message is added here because we do not consider the sensor metadata mandatory
		
		
		// ssn:featureOfInterest
		if (conf.get(SSNMapping.MAPPING_FEATURE_OF_INTEREST) != null) {
			String foi = null;
			if (((String)conf.get("ssn:featureOfInterest")).startsWith("http://")) {
				foi = namespace + "/foi/" + tuple.hashCode();
			}
			else {
				foi = (String)conf.get("ssn:featureOfInterest");
			}
			// e.g. http://example.org/obs/0001 ssn:featureOfInterest http://example.org/foi/0235
			graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
				ResourceFactory.createProperty(SSNMapping.NS_SSN, "featureOfInterest"), 
				ResourceFactory.createResource(foi)).asTriple());
		} // No error message is added here because we do not consider the sensor metadata mandatory
		
		collector.emit(new Values(observationId, System.currentTimeMillis(), graph));
		collector.ack(tuple);
	}


	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("name", "timestamp", "graph"));
	}
	

}
