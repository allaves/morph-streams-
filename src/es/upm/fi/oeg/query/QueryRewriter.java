package es.upm.fi.oeg.query;

import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator;

public class QueryRewriter {
	
	private QueryEvaluator queryEvaluator;
	
	public QueryRewriter() {
		// The QueryEvaluator in morph-streams requires a systemId to load the config
		queryEvaluator = new QueryEvaluator("default");
	}

}
