package es.upm.fi.oeg.query;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import scala.Predef;
import scala.collection.JavaConverters;
import scala.collection.convert.Decorators.AsScala;
import scala.collection.immutable.HashSet;
import scala.collection.immutable.Set;

import com.esotericsoftware.minlog.Log;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.compose.MultiUnion;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpBGP;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpGroup;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpLeftJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.algebra.op.OpService;
import com.hp.hpl.jena.sparql.algebra.op.OpUnion;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.main.StageBuilder;

import es.upm.fi.oeg.morph.common.TimeUnit;
import es.upm.fi.oeg.morph.r2rml.R2rmlReader;
import es.upm.fi.oeg.morph.stream.algebra.AlgebraOp;
import es.upm.fi.oeg.morph.stream.algebra.MultiUnionOp;
import es.upm.fi.oeg.morph.stream.algebra.ProjectionOp;
import es.upm.fi.oeg.morph.stream.algebra.RelationOp;
import es.upm.fi.oeg.morph.stream.algebra.SelectionOp;
import es.upm.fi.oeg.morph.stream.algebra.UnaryOp;
import es.upm.fi.oeg.morph.stream.algebra.WindowOp;
import es.upm.fi.oeg.morph.stream.algebra.WindowSpec;
import es.upm.fi.oeg.morph.stream.algebra.xpr.UnassignedVarXpr;
import es.upm.fi.oeg.morph.stream.algebra.xpr.Xpr;
import es.upm.fi.oeg.morph.stream.evaluate.QueryEvaluator;
import es.upm.fi.oeg.morph.stream.query.Modifiers.OutputModifier;
import es.upm.fi.oeg.morph.stream.query.Modifiers;
import es.upm.fi.oeg.morph.stream.query.SourceQuery;
import es.upm.fi.oeg.morph.stream.query.SqlQuery;
import es.upm.fi.oeg.morph.stream.rewriting.QueryReordering;
import es.upm.fi.oeg.morph.stream.rewriting.QueryRewriting;
import es.upm.fi.oeg.morph.voc.RDF;
import es.upm.fi.oeg.sparqlstream.OpStreamGraph;
import es.upm.fi.oeg.sparqlstream.SparqlStream;
import es.upm.fi.oeg.sparqlstream.StreamAlgebra;
import es.upm.fi.oeg.sparqlstream.StreamQuery;
import es.upm.fi.oeg.sparqlstream.syntax.ElementStreamGraph;
import es.upm.fi.oeg.sparqlstream.syntax.ElementTimeValue;
import es.upm.fi.oeg.sparqlstream.syntax.ElementTimeWindow;
import es.upm.fi.oeg.stream.Stream;
import es.upm.fi.oeg.stream.StreamHandler;
import es.upm.fi.oeg.utils.SSNMapping;
import es.upm.fi.oeg.utils.ScalaConverter;

public class QueryRewriter {
	
	private QueryEvaluator queryEvaluator;
	private QueryRewriting queryRewriting;
	private ScalaConverter scalaConverter;
	
	// Support for one SSNMapping
	private SSNMapping ssnMapping;
	
	private ArrayList<Stream> streams; 
	
	// morph-streams query rewriting attributes
	private String queryClass;
	
	/*
	 * This is a Java version of the QueryRewriting class (in Scala) of morph-streams - Applying the KISS concept.
	 * See https://github.com/jpcik/morph-streams/blob/master/query-rewriting/src/main/scala/es/upm/fi/oeg/morph/stream/rewriting/QueryRewriting.scala
	 */
	public QueryRewriter() {
		streams = new ArrayList<Stream>();
		scalaConverter = new ScalaConverter();
	}
	
	
	public AlgebraOp queryToAlgebra(Query query) {
		// Get the mapping related to the stream in the query. TODO: support multiple mappings for multiple streams.
		streams.addAll(QueryHandler.getInstance().getStreams(query.getQueryId()));
		ssnMapping = streams.get(0).getSSNMapping();
		return translateToAlgebra(query.getStreamQuery());
	}
	
	
	private AlgebraOp translateToAlgebra(StreamQuery query) {
		long ini = System.currentTimeMillis();
		Op op = StreamAlgebra.compile(query);
		long span1 = System.currentTimeMillis() - ini;
		long span2 = System.currentTimeMillis() - ini;
		
		// Implementing...
		AlgebraOp algebraOp = navigate(op, query);
		
		return algebraOp;
	}

	/*
	 * Under construction
	 */
	private AlgebraOp navigate(Op op, StreamQuery query) {
		AlgebraOp algebraOp = null;
		if (op instanceof OpBGP) {
			return processBGP((OpBGP) op, query);
		}
		else if (op instanceof OpProject) {
			AlgebraOp auxOp = navigate(((OpProject) op).getSubOp(), query);
			Map<String, Xpr> expressions = new HashMap<String, Xpr>();
			for (Var var : ((OpProject) op).getVars()) {
				expressions.put(var.getVarName(), UnassignedVarXpr.copy());
			}
			return new ProjectionOp(scalaConverter.convert(expressions), auxOp, query.isDistinct());
		}
		else if (op instanceof OpJoin) {
			
		}
		else if (op instanceof OpLeftJoin) {
			
		}
		else if (op instanceof OpFilter) {
			
		}
		else if (op instanceof OpService) {
			
		}
		else if (op instanceof OpDistinct) {
			return navigate(((OpDistinct) op).getSubOp(), query);
		}
		else if (op instanceof OpExtend) {
			
		}
		else if (op instanceof OpGroup) {
			
		}
		else if (op instanceof OpUnion) {
			
		}
		else if (op instanceof OpStreamGraph) {
			
		}
		else if (op instanceof OpGraph) {
			
		}
		else {
			
		}
		return algebraOp;
	}


	/*
	 * UNDER CONSTRUCTION
	 * Method that processes Basic Graph Patterns (BGP)
	 * TODO: Check how this is implemented at Linked Data Fragments - http://linkeddatafragments.org/
	 */
	private AlgebraOp processBGP(OpBGP bgpOp, StreamQuery query) {
		Map<Triple, AlgebraOp> bgpOperators = new HashMap<Triple, AlgebraOp>();
		AlgebraOp op = null;
		MultiUnionOp multiUnionOp = null;
		// Traverse through the triples in a BGP - For each triple...
		for (Triple t : bgpOp.getPattern().getList()) {
			// If the predicate of the triple is a variable...
			if (t.getPredicate().isVariable()) {
				Log.debug("Processing triple with variable predicate: " + t);
				// E.g. "?observation ?temporalProperty \"2015-06-17T12:00:000Z\"^^xsd:dateTime. "
				// val poMaps = reader.allPredicates - List with all predicates of the mapping
				Map<String, String> predicateObjectMap = ssnMapping.getAllPredicates();
				ArrayList<ProjectionOp> childrenProjection = new ArrayList<ProjectionOp>();
				Map<String, AlgebraOp> childrenProjectionMap = new HashMap<String, AlgebraOp>();
				// val children = poMaps.map
				for (Entry<String, String> predicateObject : predicateObjectMap.entrySet()) {
					ProjectionOp projectionOp = processPredicateObjectMap(t, predicateObject.getKey(), predicateObject.getValue(), query);
					childrenProjection.add(projectionOp);
					childrenProjectionMap.put(projectionOp.getRelation().id(), projectionOp);
				}
				multiUnionOp = new MultiUnionOp(scalaConverter.convert(childrenProjectionMap));
				bgpOperators.put(t, multiUnionOp);
			}
			else if (t.getPredicate().hasURI(RDF.typeProp().getURI())) {
				Log.debug("Processing rdfs:type triple: " + t);
				// E.g. "?observation a ssn:Observation. "
				// val tMaps = reader.filterBySubject(t.getObject.getURI)
				List<String> subjectMappings = ssnMapping.getMappingsBySubject(t.getObject().getURI());
				
				
			}
			else {
				// E.g. "?observation ssn:observedBy ?sensor. "
				// val poMaps = reader.filterByPredicate(t.getPredicate.getURI)
				List<String> predicateMappings = ssnMapping.getMappingsByPredicate(t.getPredicate().getURI());
				
				
			}
			
		}
		return op;
	}

	/*
	 * Returns a projection operator as a result of ...
	 */
	private ProjectionOp processPredicateObjectMap(Triple t, String predicate, String object, StreamQuery query) {
		Log.debug("Processing triple: " + t);
		List<ElementStreamGraph> streams = query.getStreams();
		// RelationOp or SelectionOp
		// Support for 1 stream. TODO: add support for more streams.
		UnaryOp unary = createRelation(predicate, object, streams.get(0));
		SelectionOp selectionOp = null;
//		if (t.getObject().isURI() || t.getObject().isLiteral()) {
//			selectionOp = createSelection(t, t.getObject().toString(), unary);
//			return createProjection(t, predicate, object, selectionOp);
//		}
//		else if (t.getSubject().isURI()) {
//			selectionOp = createSelection(t, t.getSubject().toString(), unary);
//			return createProjection(t);
//		}
		//return createProjection(t, predicate, object, unary);
		return null;
	}

	/*
	 * Returns a unary window operator with the time window specifications
	 */
	private UnaryOp createRelation(String predicate, String object, ElementStreamGraph stream) {
		// We assume that all declared streams have a time window in a SPARQLStream query
		Log.debug("Creating window for " + stream.getUri());
		ElementTimeWindow window = (ElementTimeWindow) stream.window();
		long slide = 0L;
		TimeUnit slideUnit = null;
		if (window.slide() != null) {
			slide = window.slide().time();
			slideUnit = window.slide().getUnit();
		}
		WindowSpec windowSpec = new WindowSpec(stream.getUri(), window.from().time(), window.from().unit(), 0, null, slide, slideUnit);
		// Be careful here, dude! http://stackoverflow.com/questions/3025291/example-of-using-scala-collection-immutable-set-from-java
		//Set<String> pkSet = new HashSet<String>();
		Set<String> pkSet2 = (Set<String>) new Set.Set1<String>(SSNMapping.MAPPING_OBSERVATION_ID);
		//pkSet.$plus(SSNMapping.MAPPING_OBSERVATION_ID); // TODO: NoSuchMethodError!
		return new WindowOp(stream.getUri(), "extentName", pkSet2, windowSpec);
	}


	private SourceQuery translate(String queryString) {
		return translate(SparqlStream.parse(queryString));
	}

	private SourceQuery translate(StreamQuery query) {
		AlgebraOp algebra = translateToAlgebra(QueryReordering.reorder(query));
		Map<String, String> queryVariables = new HashMap<String, String>();
		// Support for SELECT queries
		for (Var var : query.getProjectVars()) {
			queryVariables.put(var.getName().toLowerCase(), null);
		}
		// TODO: support for modifiers, i.e. Dstream, Istream, Rstream.
		return transform(algebra, queryVariables, null);
	}

	private SourceQuery transform(AlgebraOp algebra, Map<String, String> queryVariables, OutputModifier[] modifiers) {
		SourceQuery translatedQuery = null;
		try {
			Class<?> theClass = Class.forName(queryClass);
			translatedQuery = (SourceQuery) theClass.getDeclaredConstructor(AlgebraOp.class, Modifiers.OutputModifier.class).newInstance(algebra, modifiers);
		} catch (ClassNotFoundException e) {
			Log.error("Unable to use adapter query " + queryClass);
			e.printStackTrace();
		} catch (InstantiationException e) {
			Log.error("Unable to instantiate query " + queryClass);
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			Log.error("Unable to use adapter query " + queryClass);
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return translatedQuery;
	}


}
