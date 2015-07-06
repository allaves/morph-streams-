package es.upm.fi.oeg.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SSNMapping {
	
	public final static String MAPPING_OBSERVATION_ID = "observationId";
	public final static String MAPPING_OBSERVED_PROPERTY = "ssn:observedProperty";
	public final static String MAPPING_DATA_VALUE = "ssn:hasDataValue";
	public final static String MAPPING_OBSERVATION_SAMPLING_TIME = "ssn:observationSamplingTime";
	public final static String MAPPING_OBSERVATION_RESULT_TIME = "ssn:observationResultTime";
	public final static String MAPPING_FEATURE_OF_INTEREST = "ssn:featureOfInterest";
	public final static String MAPPING_OBSERVED_BY = "ssn:observedBy";
	public final static String MAPPING_GEOSPARQL_WKT = "geosparql:asWKT";
	public final static String MAPPING_LATITUDE = "lat";
	public final static String MAPPING_LONGITUDE = "lon";
	public final static String MAPPING_CRS = "crs";
	
	public final static String OBSERVED_PROPERTY = "property";
	
	public final static String NS_SSN = "http://purl.oclc.org/NET/ssnx/ssn#";
	public final static String NS_GEOSPARQL = "http://www.opengis.net/ont/geosparql#";
	public final static String NS_RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
	
	private HashMap<String, String> mapping;
	
	public SSNMapping(HashMap<String, String> mapping) {
		this.mapping = mapping;
	}
	
	public HashMap<String, String> getMappings() {
		return mapping;
	}

	public List<String> getMappingsBySubject(String subjectUri) {
		List<String> mappingsBySubject = new ArrayList<String>();
		if (subjectUri.equalsIgnoreCase(NS_SSN + "Observation")) {
			if (mapping.containsKey(MAPPING_OBSERVATION_ID)) {
				mappingsBySubject.add(mapping.get(MAPPING_OBSERVATION_ID));
			}
			if (mapping.containsKey(MAPPING_OBSERVED_PROPERTY)) {
				mappingsBySubject.add(mapping.get(MAPPING_OBSERVED_PROPERTY));
			}
			if (mapping.containsKey(MAPPING_OBSERVATION_SAMPLING_TIME)) {
				mappingsBySubject.add(mapping.get(MAPPING_OBSERVATION_SAMPLING_TIME));
			}
			else if (mapping.containsKey(MAPPING_OBSERVATION_RESULT_TIME)) {
				mappingsBySubject.add(mapping.get(MAPPING_OBSERVATION_RESULT_TIME));
			}
			if (mapping.containsKey(MAPPING_FEATURE_OF_INTEREST)) {
				mappingsBySubject.add(mapping.get(MAPPING_FEATURE_OF_INTEREST));
			}
			if (mapping.containsKey(MAPPING_OBSERVED_BY)) {
				mappingsBySubject.add(mapping.get(MAPPING_OBSERVED_BY));
			}
		}
		else if (subjectUri.equalsIgnoreCase(NS_SSN + "SensorOutput")) {
			if (mapping.containsKey(MAPPING_DATA_VALUE)) {
				mappingsBySubject.add(mapping.get(MAPPING_DATA_VALUE));
			}
		}
		else if (subjectUri.equalsIgnoreCase(NS_GEOSPARQL + "Geometry")) {
			if (mapping.containsKey(MAPPING_GEOSPARQL_WKT)) {
				mappingsBySubject.add(mapping.get(MAPPING_GEOSPARQL_WKT));
			}
			else {
				if (mapping.containsKey(MAPPING_OBSERVATION_ID)) {
					mappingsBySubject.add(mapping.get(MAPPING_OBSERVATION_ID));
				}
				if (mapping.containsKey(MAPPING_OBSERVED_PROPERTY)) {
					mappingsBySubject.add(mapping.get(MAPPING_OBSERVED_PROPERTY));
				}
				if (mapping.containsKey(MAPPING_OBSERVATION_SAMPLING_TIME)) {
					mappingsBySubject.add(mapping.get(MAPPING_OBSERVATION_SAMPLING_TIME));
				}
			}
		}
		return mappingsBySubject;
		
		
	}

	/*
	 * Returns the list of mappings related to a predicate
	 */
	public List<String> getMappingsByPredicate(String predicateUri) {
		List<String> mappingsByPredicate = new ArrayList<String>();
		if (predicateUri.equalsIgnoreCase(NS_SSN + "observedProperty")) {
			if (mapping.containsKey(MAPPING_OBSERVED_PROPERTY)) {
				mappingsByPredicate.add(mapping.get(MAPPING_OBSERVED_PROPERTY));
			}
		}
		else if (predicateUri.equalsIgnoreCase(NS_SSN + "hasDataValue")) {
			if (mapping.containsKey(MAPPING_DATA_VALUE)) {
				mappingsByPredicate.add(mapping.get(MAPPING_DATA_VALUE));
			}
		}
		else if (predicateUri.equalsIgnoreCase(NS_SSN + "observationSamplingTime")) {
			if (mapping.containsKey(MAPPING_OBSERVATION_SAMPLING_TIME)) {
				mappingsByPredicate.add(mapping.get(MAPPING_OBSERVATION_SAMPLING_TIME));
			}
		}
		else if (predicateUri.equalsIgnoreCase(NS_SSN + "observationResultTime")) {
			if (mapping.containsKey(MAPPING_OBSERVATION_RESULT_TIME)) {
				mappingsByPredicate.add(mapping.get(MAPPING_OBSERVATION_RESULT_TIME));
			}
		}
		else if (predicateUri.equalsIgnoreCase(NS_SSN + "featureOfInterest")) {
			if (mapping.containsKey(MAPPING_FEATURE_OF_INTEREST)) {
				mappingsByPredicate.add(mapping.get(MAPPING_FEATURE_OF_INTEREST));
			}
		}
		else if (predicateUri.equalsIgnoreCase(NS_SSN + "observedBy")) {
			if (mapping.containsKey(MAPPING_OBSERVED_BY)) {
				mappingsByPredicate.add(mapping.get(MAPPING_OBSERVED_BY));
			}
		}
		else if (predicateUri.equalsIgnoreCase(NS_GEOSPARQL + "asWKT")) {
			if (mapping.containsKey(MAPPING_GEOSPARQL_WKT)) {
				mappingsByPredicate.add(mapping.get(MAPPING_GEOSPARQL_WKT));
			}
			else if (mapping.containsKey(MAPPING_LATITUDE) && mapping.containsKey(MAPPING_LONGITUDE)) {
				mappingsByPredicate.add(MAPPING_LATITUDE);
				mappingsByPredicate.add(MAPPING_LONGITUDE);
				if (mapping.containsKey(MAPPING_CRS)) {
					mappingsByPredicate.add(MAPPING_CRS);
				}
			}
		}
		return mappingsByPredicate;
	}

	/*
	 * Returns a map containing all predicates
	 */
	public Map<String, String> getAllPredicates() {
		return getMappings();
	}
	
	

}
