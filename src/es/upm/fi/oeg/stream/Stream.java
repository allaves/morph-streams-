package es.upm.fi.oeg.stream;

import java.net.URL;


public class Stream {
	
	private String id;
	private URL url;
	private URL urlLogin;
	private long millisecondsRate;
	private String format;
	private String topic;
	
	// In case of having an API with authentication
	private String user;
	private String password;
	
	
	public enum FORMAT {
		CSV_LINE, CSV_DOCUMENT, RDF, JSON
	}
	
	public Stream(URL url, int millisecondsRate, FORMAT format, String topic) {
		this.id = url + "--" + Integer.toString(millisecondsRate);
		this.url = url;
		this.millisecondsRate = millisecondsRate;
		this.format = format.toString();
		this.topic = topic;
	}
	
	public Stream(URL url, int millisecondsRate, FORMAT format, String topic, String user, String password, URL urlLogin) {
		this.id = url + "--" + Integer.toString(millisecondsRate);
		this.url = url;
		this.millisecondsRate = millisecondsRate;
		this.format = format.toString();
		this.topic = topic;
		this.user = user;
		this.password = password;
		this.urlLogin = urlLogin;
	}

	public String getId() {
		return id;
	}
	
	public URL getUrl() {
		return url;
	}
	
	public long getMillisecondsRate() {
		return millisecondsRate;
	}
	
	public String getFormat() {
		return format;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public String getUser() {
		return user;
	}
	
	public String getPassword() {
		return password;
	}
	
	public URL getUrlLogin() {
		return urlLogin;
	}
}
