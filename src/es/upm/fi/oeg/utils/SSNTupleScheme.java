package es.upm.fi.oeg.utils;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import backtype.storm.spout.MultiScheme;
import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SSNTupleScheme implements Scheme {

	@Override
	public List<Object> deserialize(byte[] message) {
		String ssnObs;
		try {
			ssnObs = new String(message, "UTF-8");
		    String[] tuple = ssnObs.split(",");
		    String observationResultTime = tuple[0].trim().replace("[", "");
		    String observationSamplingTime = tuple[1].trim();
		    String value = tuple[2].trim();
		    String network = tuple[3].trim();
		    String platform = tuple[4].trim();
		    String sensor = tuple[5].trim();
		    String phenomenon = tuple[6].trim();
		    String lat = tuple[7].trim();
		    String lon = tuple[8].trim();
		    // If a tuple is not annotated (has a field "observedProperty"), the field "phenomenon" is used as the observed property.
		    if (tuple.length > 9) {
		    	String observedProperty = tuple[9].trim().replace("]", "");
		    	return new Values(observationResultTime, observationSamplingTime, value, network, platform, sensor, phenomenon, lat, lon, observedProperty);
		    }
		    else {
		    	return new Values(observationResultTime, observationSamplingTime, value, network, platform, sensor, phenomenon, lat, lon, phenomenon);
		    }
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}
	

	@Override
	public Fields getOutputFields() {
		return new Fields("observationResultTime", "observationSamplingTime", "value", 
				"network", "platform", "sensor", "phenomenon", "lat", "lon", "observedProperty");
	}

}
