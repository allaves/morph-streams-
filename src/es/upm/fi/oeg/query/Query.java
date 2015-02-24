package es.upm.fi.oeg.query;

import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp;
import es.upm.fi.oeg.morph.stream.query.SqlQuery;

public class Query {

	private String stringQuery;
	private SqlQuery sqlQuery;
	
	public Query(String stringQuery) {
		this.stringQuery = stringQuery;
		// AlgebraOp is a Trait in Scala, which corresponds to a Java interface.
		AlgebraOp algebraOp = new AlgebraOp();
		OutputModifier outputModifier[] = new OutputModifier[]();
		this.sqlQuery = new SqlQuery(algebraOp, outputModifier);
	}

	public SqlQuery getSqlQuery() {
		return sqlQuery;
	}
	
	public String getStringQuery() {
		return stringQuery;
	}

	public void setStringQuery(String stringQuery) {
		this.stringQuery = stringQuery;
	}
	
	
}
