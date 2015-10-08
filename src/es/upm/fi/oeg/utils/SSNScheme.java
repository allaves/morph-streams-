package es.upm.fi.oeg.utils;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SSNScheme implements Scheme {

	@Override
	public List<Object> deserialize(byte[] message) {
		String ssnObs;
		try {
			ssnObs = new String(message, "UTF-8");
		    String[] tuple = ssnObs.split(",");
		    String name = tuple[0].trim().replace("[", "");
		    String observationResultTime = tuple[1].trim();
		    String graph = tuple[2].trim();
		    String observedProperty = tuple[3].trim().replace("]", "");
		    return new Values(name, observationResultTime, graph, observedProperty);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	

	@Override
	public Fields getOutputFields() {
		return new Fields("name", "observationResultTime", "graph", "observedProperty");
	}

}
