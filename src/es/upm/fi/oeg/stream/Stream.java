package es.upm.fi.oeg.stream;

import java.net.URL;

import es.upm.fi.oeg.utils.SSNMapping;


public class Stream {
	
	private String id;
	private URL url;
	private URL urlLogin;
	private long millisecondsRate;
	private String format;
	private String kafkaTopic;
	private SSNMapping ssnMapping;
	
	// In case of having an API with authentication
	private String user;
	private String password;
	
	// In case of having a RabbitMQ data source
	private String rabbitMQQueue;
	
	
	public enum FORMAT {
		CSV_LINE, CSV_DOCUMENT, RDF, JSON, SENSOR_CLOUD
	}
	
	public Stream(URL url, int millisecondsRate, FORMAT format, String kafkaTopic) {
		this.id = url.toString();
		this.url = url;
		this.millisecondsRate = millisecondsRate;
		this.format = format.toString();
		this.kafkaTopic = kafkaTopic;
	}
	
	public Stream(URL url, int millisecondsRate, FORMAT format, String kafkaTopic, String user, String password, URL urlLogin) {
		this.id = url.toString();
		this.url = url;
		this.millisecondsRate = millisecondsRate;
		this.format = format.toString();
		this.kafkaTopic = kafkaTopic;
		this.user = user;
		this.password = password;
		this.urlLogin = urlLogin;
	}
	
	public Stream(URL url, int millisecondsRate, FORMAT format, String kafkaTopic, String user, String password, String rabbitMQQueue, SSNMapping ssnMapping) {
		this.id = url.toString();
		this.url = url;
		this.millisecondsRate = millisecondsRate;
		this.format = format.toString();
		this.kafkaTopic = kafkaTopic;
		this.user = user;
		this.password = password;
		this.rabbitMQQueue = rabbitMQQueue;
		this.ssnMapping = ssnMapping;
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
	
	public String getKafkaTopic() {
		return kafkaTopic;
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
	
	public String getRabbitMQQueue() {
		return rabbitMQQueue;
	}
	
	public SSNMapping getSSNMapping() {
		return ssnMapping;
	}
}
