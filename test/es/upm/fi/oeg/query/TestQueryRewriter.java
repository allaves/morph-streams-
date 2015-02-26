package es.upm.fi.oeg.query;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.typesafe.config.ConfigFactory;

import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp;

public class TestQueryRewriter {
	
	private QueryRewriter queryRewriter;
	private String queryString;
	private Query query;

	@Before
	public void setUp() {
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
	}

	@Test
	public void testQueryToAlgebra() {
		query = new Query(queryString);
		
		// TODO: The application stops without any visible error or warning!
		queryRewriter = new QueryRewriter();
		AlgebraOp algebraOp = queryRewriter.queryToAlgebra(query);
		algebraOp.display();
		assertTrue(true);
	}

}
