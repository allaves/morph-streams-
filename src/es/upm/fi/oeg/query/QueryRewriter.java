package es.upm.fi.oeg.query;

import es.upm.fi.oeg.morph.r2rml.R2rmlReader;
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp;
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator;
import es.upm.fi.oeg.morph.stream.rewriting.QueryRewriting;

public class QueryRewriter {
	
	private QueryEvaluator queryEvaluator;
	private QueryRewriting queryRewriting;
	
	public QueryRewriter() {
		// TESTING: The QueryEvaluator in morph-streams requires a systemId to load the config
		//queryEvaluator = new QueryEvaluator("default");	// The application ends without any error or warning
		// TESTING: The QueryRewriting in morph-streams requires a InputStream or R2rmlReader and a systemId to load the config
		// NOT WORKING
		queryRewriting = new QueryRewriting(new R2rmlReader(""), null);	// The application ends without any error or warning
		
	}
	
	/*
	 * Testing the translation from Query to Algebra
	 * NOT WORKING
	 */
	public AlgebraOp queryToAlgebra(Query query) {
		return queryRewriting.translateToAlgebra(query.getStreamQuery());
	}

}
