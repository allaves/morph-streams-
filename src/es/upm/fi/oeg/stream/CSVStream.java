package es.upm.fi.oeg.stream;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;

public class CSVStream extends Stream {
	
	private char delimiter;
	private String[] fieldNames;
	private XSDDatatype[] fieldDataTypes;
	private HashMap<String, String> ssnMapping;
	
	public CSVStream(URL url, int millisecondsRate, FORMAT format,	String topic, char delimiter, 
			String[] fieldNames, XSDDatatype[] fieldDataTypes, HashMap<String, String> ssnMapping) {
		super(url, millisecondsRate, format, topic);
		this.delimiter = delimiter;
		this.fieldNames = fieldNames;
		this.fieldDataTypes = fieldDataTypes;
		this.ssnMapping = ssnMapping;
	}
	
	public char getDelimiter() {
		return delimiter;
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public XSDDatatype[] getFieldDataTypes() {
		return fieldDataTypes;
	}

	public HashMap<String, String> getSsnMapping() {
		return ssnMapping;
	}

}
