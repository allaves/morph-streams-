package es.upm.fi.oeg.query;

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import es.upm.fi.oeg.morph.stream.query.SqlQuery;
import es.upm.fi.oeg.stream.StreamHandler;

public class QueryHandler {

	private Logger log = LoggerFactory.getLogger(QueryHandler.class);
	
	private HashMap<Integer, Query> queryRegistry;
	
	/*
	 * Singleton stuff starts
	 */
	private static QueryHandler instance;
	
	private QueryHandler() {
		queryRegistry = new HashMap<Integer, Query>();
		
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
	public Integer registerQuery(String stringQuery) {
		// TODO: Checks that the SPARQLStream syntax is correct
		
		// Puts query into the registry
		int queryId = stringQuery.hashCode();
		if (!queryRegistry.containsKey(queryId)) {
			queryRegistry.put(queryId, new Query(stringQuery));
			log.info("New query registered with id: " + queryId);
			log.info(stringQuery);
			// TODO: Checks that the streams are registered
			// TODO: Converts query to algebra
			// TODO: Passes topology to the TopologyManager
			
			return queryId;
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
	
	
	
	
	
}
