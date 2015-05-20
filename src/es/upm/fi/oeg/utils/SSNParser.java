package es.upm.fi.oeg.utils;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Factory;
import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

public class SSNParser {
	
	public SSNParser() {}
	
	/*
	 * Creates SSN graph from observation properties passed as strings
	 * TODO: add support for sensor, platform, foi 
	 */
	public Graph createSSNGraph(String namespace, String observationId, String observedProperty, String timestamp, String value, String lat, String lon, String crs) {
		Graph graph = Factory.createDefaultGraph();
		// ssn:Observation
		// e.g. http://example.org/obs/0001 rdf:type ssn:Observation
		graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
			ResourceFactory.createProperty(SSNMapping.NS_RDF, "type"), 
			ResourceFactory.createResource(SSNMapping.NS_SSN + "Observation")).asTriple());
		
		// ssn:observedProperty
		// e.g. http://example.org/obs/0001 ssn:observedProperty http://example.org/observedProperty/vehiclePosition
		graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
			ResourceFactory.createProperty(SSNMapping.NS_SSN, "observedProperty"), 
			ResourceFactory.createResource(observedProperty)).asTriple());
		
		// ssn:hasDataValue
		// e.g. http://example.org/obs/0001 ssn:observationResult http://example.org/obs/0001/output
		graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
			ResourceFactory.createProperty(SSNMapping.NS_SSN, "observationResult"), 
			ResourceFactory.createResource(observationId + "/output")).asTriple());
		// e.g. http://example.org/obs/0001/output ssn:hasValue "17.8"
		// TODO: infer the type of value to add a typed literal, e.g. xsd:float, instead of a plain literal
		graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId + "/output"), 
			ResourceFactory.createProperty(SSNMapping.NS_SSN, "hasValue"), 
			ResourceFactory.createPlainLiteral(value)).asTriple());
		// TODO: unit of measurement, e.g. qudt
		
		// ssn:observationResultTime
		// TODO: format the input date according to xsd:dateTime
		// e.g. http://example.org/obs/0001 ssn:observationSamplingTime "2002-05-30T09:30:10+02:00"^^xsd:dateTime
		graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
			ResourceFactory.createProperty(SSNMapping.NS_SSN, "observationResultTime"), 
			ResourceFactory.createTypedLiteral(timestamp, XSDDatatype.XSDdateTime)).asTriple());
		
		// geosparql:asWKT
		// We assume that the spatial location is a point given by two coordinates x and y in a String, i.e. "x y"
		// TODO: add support for lines and polygons.
		// TODO: add support for elevation (z).
		String latLon = lat + " " + lon;
		if (crs == null) {
			// If no crs is provided, we assume that the data follows the global default EPSG:4326.
			crs = "http://www.opengis.net/def/crs/EPSG/0/4326";
		}
		String geom = namespace + "geom/" + lat + lon;
		// e.g. http://example.org/obs/0001 geosparql:hasGeometry http://example.org/geom/0002
		graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(observationId), 
			ResourceFactory.createProperty(SSNMapping.NS_GEOSPARQL, "hasGeometry"), 
			ResourceFactory.createResource(geom)).asTriple());
		// e.g. http://example.org/geom/0002 geosparql:asWKT "<http://www.opengis.net/def/crs/EPSG/0/4258> POINT(27.7488 -18.0678)"^^geosparql:wktLiteral
		graph.add(ResourceFactory.createStatement(ResourceFactory.createResource(geom), 
			ResourceFactory.createProperty(SSNMapping.NS_GEOSPARQL, "asWKT"), 
			ResourceFactory.createPlainLiteral("<" + crs + "> POINT(" + latLon + ")")).asTriple());
		// TODO: create a wktLiteral instead of a plain literal. Problem: wktLiteral is not a RDFDataType.
		
		return graph;
	}

}
