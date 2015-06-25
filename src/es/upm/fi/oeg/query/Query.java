package es.upm.fi.oeg.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import es.upm.fi.oeg.morph.stream.query.SourceQuery;
import es.upm.fi.oeg.sparqlstream.SparqlStream;
import es.upm.fi.oeg.sparqlstream.StreamQuery;
import es.upm.fi.oeg.sparqlstream.syntax.ElementStreamGraph;
import es.upm.fi.oeg.stream.Stream;
import es.upm.fi.oeg.utils.SSNMapping;

public class Query {

	private String stringQuery;
	private StreamQuery streamQuery;
	private SourceQuery sourceQuery;
	private String queryId;
	//private SqlQuery sqlQuery;
	
	public Query(String stringQuery) {
		// The query identifier is at this moment a hash of the query string.
		// In future versions, the id could be related to the query algebra, so that the same query written differently still is represented as the same query in the registry.
		queryId = String.valueOf(stringQuery.hashCode());
		// Checks if the query is syntax is valid SPARQLStream and returns a StreamQuery
		streamQuery = SparqlStream.parse(stringQuery);
		
		this.stringQuery = stringQuery;
		// AlgebraOp is a Trait in Scala, which corresponds to a Java interface.
//		AlgebraOp algebraOp = new AlgebraOp();
//		OutputModifier outputModifier[] = new OutputModifier[]();
//		this.sqlQuery = new SqlQuery(algebraOp, outputModifier);
	}
	
	public String getStringQuery() {
		return stringQuery;
	}

	public void setStringQuery(String stringQuery) {
		this.stringQuery = stringQuery;
	}
	
	public StreamQuery getStreamQuery() {
		return streamQuery;
	}

	/*
	 * Returns the stream sources in the query as strings
	 */
	public Collection<String> getStreamUris() {
		Collection<String> streams = new ArrayList<String>();
		for (ElementStreamGraph streamGraph : streamQuery.getStreams()) {
			streams.add(streamGraph.getUri());
		}
		return streams;
	}
	
	
	public String getQueryId() {
		return queryId;
	}
	
	
	
	
	
}
