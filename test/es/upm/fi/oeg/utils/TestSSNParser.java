package es.upm.fi.oeg.utils;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class TestSSNParser {

	@Before
	public void setUp() throws Exception {
		
	}

	@Test
	public void testCreateSSNGraph() {
		String namespace = "http://sensorcloud.linkeddata.es/";
		String observedProperty = namespace + "observedProperty/relative-humidity";
		String timestamp = "2015-04-13T11:11:59.000";
		String value = "100.0";
		String lat = "-42.027000";
		String lon = "148.073833";
		String crs = null;
		String observationId = namespace + "obs/" + "relative-humidity" + "-" + timestamp;
		SSNParser parser = new SSNParser();
		System.out.println(parser.createSSNGraph(namespace, observationId, observedProperty, timestamp, value, lat, lon, crs).toString());
	}

}
