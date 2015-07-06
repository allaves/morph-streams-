package es.upm.fi.oeg.query;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.util.HashMap;

import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.ConfigFactory;

import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp;
import es.upm.fi.oeg.morph.stream.query.SqlQuery;
import es.upm.fi.oeg.stream.Stream;
import es.upm.fi.oeg.stream.Stream.FORMAT;
import es.upm.fi.oeg.stream.StreamHandler;
import es.upm.fi.oeg.utils.SSNMapping;

public class TestQueryRewriter {
	
	private QueryRewriter queryRewriter;
	private String queryString;
	private String q1;
	private Query query;
	
	// Sensor Cloud credentials
	private String user;
	private String password;
	private String queue;

	@Before
	public void setUp() throws Exception {
		queryString = "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#> "
				+ "PREFIX qudt: <http://data.nasa.gov/qudt/owl/qudt#> "
				+ "PREFIX emt: <http://emt.linkeddata.es/data#> "
				+ "SELECT ?timeto ?obs ?av "
				+ "FROM NAMED STREAM <http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61> [NOW - 300 S] "
				+ "WHERE { "
				+ "?obs a emt:BusObservation. "
				+ "?obs ssn:observationResult ?output. "
				+ "?output emt:timeToBusValue ?av. "
				+ "?av qudt:numericValue ?timeto. "
				+ "}";
		
		q1 = "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#> "
				+ "SELECT DISTINCT ?sensor ?value "
				+ "FROM NAMED STREAM <http://sensorcloud.linkeddata.es/obs/> [NOW - 1 HOURS] "
				+ "WHERE { "
				+ "?observation ?a ssn:Observation. "
				+ "?observation ssn:observedBy ?sensor. "
				+ "?observation ssn:observationResult ?result. "
				+ "?result ssn:hasValue ?value. "
				+ "}";

		
		BufferedReader br = new BufferedReader(new FileReader(new File("resources/credentials-sensor-cloud.txt")));
		// 1st line: user
		user = br.readLine();
		// 2nd line: password
		password = br.readLine();
		// 3rd line: queue
		queue = br.readLine();
		br.close();
	}

	@Test
	public void testQueryToAlgebra() {
		HashMap<String, String> mapping = new HashMap<String, String>();
		mapping.put(SSNMapping.MAPPING_OBSERVED_PROPERTY, "observedProperty");
		mapping.put(SSNMapping.MAPPING_DATA_VALUE, "value");
		mapping.put(SSNMapping.MAPPING_LATITUDE, "lat");
		mapping.put(SSNMapping.MAPPING_LONGITUDE, "lon");
		mapping.put(SSNMapping.MAPPING_OBSERVATION_RESULT_TIME, "observationSamplingTime");
		mapping.put(SSNMapping.MAPPING_OBSERVED_BY, "sensor");
		// ...
		SSNMapping ssnMapping = new SSNMapping(mapping);
		StreamHandler.getInstance().registerStreamRabbitMQ("http://sensorcloud.linkeddata.es/obs/", 0, FORMAT.SENSOR_CLOUD, "kafkaTopic", user, password, queue, ssnMapping);
		QueryHandler.getInstance().registerQuery(q1);
		
		queryRewriter = new QueryRewriter();
		query = new Query(q1);
		AlgebraOp algebraOp = queryRewriter.queryToAlgebra(query);
		algebraOp.display(); // It does not show anything
		System.out.println(algebraOp.toString());
	}

}
