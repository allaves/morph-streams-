package es.upm.fi.oeg.query;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import es.upm.fi.oeg.stream.Stream;
import es.upm.fi.oeg.stream.StreamHandler;
import es.upm.fi.oeg.stream.Stream.FORMAT;

public class TestQueryHandler {
	
	private String query;
	private String streamId;

	@Before
	public void setUp() {
		query = "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#> "
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
	}

	@Test
	public void testRegisterQuery() throws InterruptedException {
		streamId = StreamHandler.getInstance().registerStream(
				"http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61", 
				1000, FORMAT.CSV, "hsl");
		assertEquals(String.valueOf(query.hashCode()), QueryHandler.getInstance().registerQuery(query));
		assertEquals(null, QueryHandler.getInstance().registerQuery(query));
	}
	
	@Test
	public void testStreamAvailability() throws InterruptedException {
		Thread.sleep(1000);
		for (String streamId : QueryHandler.getInstance().streamAvailability(new Query(query))) {
			System.out.println(streamId);
		}
		Thread.sleep(100000);
	}
	
//	@Test
//	public void testDeregisterQuery() throws InterruptedException {
//		//Thread.sleep(15000);
//		assertTrue(QueryHandler.getInstance().deregisterQuery(String.valueOf(query.hashCode())));
//		//assertFalse(QueryHandler.getInstance().deregisterQuery(String.valueOf(query)));
//		//StreamHandler.getInstance().deregisterStream(streamId);
//	}

}
