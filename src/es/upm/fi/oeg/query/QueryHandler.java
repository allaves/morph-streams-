package es.upm.fi.oeg.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator;
import es.upm.fi.oeg.morph.stream.query.SqlQuery;
import es.upm.fi.oeg.stream.StreamHandler;

/*
 * Handles query registration and validity
 */
public class QueryHandler {

	private Logger log = LoggerFactory.getLogger(QueryHandler.class);
	
	private HashMap<String, Query> queryRegistry;
	
	
	/*
	 * Singleton stuff starts
	 */
	private static QueryHandler instance;
	
	private QueryHandler() {
		queryRegistry = new HashMap<String, Query>();
	}
	
	public static QueryHandler getInstance() {
		if (instance == null) {
			createInstance();
		}
		return instance;
	}
	
	private static void createInstance() {
		if (instance == null) {
			synchronized(StreamHandler.class) {
				if (instance == null) {
					instance = new QueryHandler();
				}
			}
		}
	}
	
	public Object clone() throws CloneNotSupportedException {
		throw new CloneNotSupportedException();
	}
	/*
	 * Singleton stuff ends
	 */
	
	
	/*
	 * Registers a query in SPARQLStream
	 * Returns the query id if successful; null if not.
	 */
	public String registerQuery(String stringQuery) {
		// Creates a new object Query
		Query query = new Query(stringQuery);
		// Puts query into the registry
		String queryId = String.valueOf(stringQuery.hashCode());
		if (!queryRegistry.containsKey(queryId)) {
			queryRegistry.put(queryId, query);
			log.info("New query registered with id: " + queryId);
			log.info(stringQuery);
			// Checks that the stream sources are registered
			if (!streamAvailability(query).isEmpty()) {
				// TODO: Converts query to algebra
				// TODO: Passes topology to the TopologyManager
				
				return String.valueOf(queryId);
			}
			else {
				log.warn("WARNING! Stream sources are not available, so no results will be returned at the moment.");
				return null;
			}
		}
		log.error("ERROR! Query already registered.");
		return null;
	}
	
	/*
	 * Stops query processing and removes the query from the registry
	 */
	public boolean deregisterQuery(String queryId) {
		// Removes the query from the registry
		if (queryRegistry.containsKey(queryId)) {
			// TODO: Stops the query processing
			
			// Removes the query from the registry
			log.info("Query deregistered: " + queryId + " - " + queryRegistry.get(queryId).getStringQuery());
			queryRegistry.remove(queryId);
			return true;
		}
		log.error("ERROR! There is no query registered with ID " + queryId);
		return false;
	}
	
	
	/*
	 * Are the data stream sources requested in the query available?
	 * Returns an array with the stream ids available if any, or an empty array.
	 */
	public Collection<String> streamAvailability(Query query) {
		ArrayList<String> availableStreams = new ArrayList<String>();
		for (String registeredStreamId : StreamHandler.getInstance().getRegisteredStreams()) {
			for (String queryStreamUrl : query.getStreams()) {
				if (registeredStreamId.contains(queryStreamUrl)) {
					availableStreams.add(registeredStreamId);
				}
			}
		}
		return availableStreams;
	}
	
	
}
