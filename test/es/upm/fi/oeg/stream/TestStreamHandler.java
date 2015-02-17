package es.upm.fi.oeg.stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.Authenticator;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URL;

import org.junit.Before;
import org.junit.Test;

import es.upm.fi.oeg.stream.Stream.FORMAT;

public class TestStreamHandler {
	
	private Stream stream1, stream2;
	
	@Before
	public void setUp() throws MalformedURLException {
		// Sets the ESA authenticator
		//Authenticator.setDefault(new MyAuthenticator());
		stream1 = new Stream(new URL("https://esa.csiro.au/aus/alertHist?queryType=latest&listTweetIds=true"), 2000, FORMAT.JSON, "test");
		//stream2 = new Stream(new URL("http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61"), 1000, FORMAT.CSV, "hsl");
	}

	@Test
	public void testRegisterStream() {
		assertTrue(StreamHandler.getInstance().registerStream(stream1));
		assertFalse(StreamHandler.getInstance().registerStream(stream1));
//		assertTrue(StreamHandler.getInstance().registerStream(stream2));
//		assertFalse(StreamHandler.getInstance().registerStream(stream2));
	}

	@Test
	public void testGetRegisteredStreams() {
		assertFalse(StreamHandler.getInstance().getRegisteredStreams().isEmpty());
		//assertEquals(StreamHandler.getInstance().getRegisteredStreams().size(), 2);
	}
	
	@Test
	public void testDeregisterStream() throws InterruptedException {
		Thread.sleep(30000);
		assertTrue(StreamHandler.getInstance().deregisterStream(stream1.getId()));
		assertFalse(StreamHandler.getInstance().deregisterStream(stream1.getId()));
//		assertTrue(StreamHandler.getInstance().deregisterStream(stream2.getId()));
//		assertFalse(StreamHandler.getInstance().deregisterStream(stream2.getId()));
	}


	

}
