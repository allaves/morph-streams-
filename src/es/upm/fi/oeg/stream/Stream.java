package es.upm.fi.oeg.stream;

import java.net.URL;

public class Stream {
	
	private String id;
	private URL url;
	private long rate;		// In milliseconds
	private String format;
	private String topic;
	
	public enum FORMAT {
		CSV, RDF, JSON
	}
	
	public Stream(URL url, int rate, FORMAT format, String topic) {
		this.id = url + "--" + Integer.toString(rate);
		this.url = url;
		this.rate = rate;
		this.format = format.toString();
		this.topic = topic;
	}
	
	public String getId() {
		return id;
	}
	
	public URL getUrl() {
		return url;
	}
	
	public long getRate() {
		return rate;
	}
	
	public String getFormat() {
		return format;
	}
	
	public String getTopic() {
		return topic;
	}
}
