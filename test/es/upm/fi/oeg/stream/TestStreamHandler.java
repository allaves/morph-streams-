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
	
	@Before
	public void setUp() {}

	@Test
	public void testRegisterStream() {
		assertEquals("https://esa.csiro.au/aus/alertHist?queryType=latest&listTweetIds=true--60000", 
				StreamHandler.getInstance().registerStreamWithFormBasedLogin(
						"https://esa.csiro.au/aus/alertHist?queryType=latest&listTweetIds=true", 
						60000, FORMAT.JSON, "test", "allaves", "cacafut1", "https://esa.csiro.au/aus/j_security_check"));
		assertEquals(null, 
				StreamHandler.getInstance().registerStreamWithFormBasedLogin(
						"https://esa.csiro.au/aus/alertHist?queryType=latest&listTweetIds=true", 
						60000, FORMAT.JSON, "test", "allaves", "cacafut1", 
						"https://esa.csiro.au/aus/j_security_check"));
//		assertEquals("http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61--1000", 
//				StreamHandler.getInstance().registerStream(
//						"http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61", 
//						1000, FORMAT.CSV_DOCUMENT, "hsl"));
//		assertEquals(null, 
//				StreamHandler.getInstance().registerStream(
//						"http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61", 
//						1000, FORMAT.CSV_DOCUMENT, "hsl"));
	}

	@Test
	public void testGetRegisteredStreams() {
		assertFalse(StreamHandler.getInstance().getRegisteredStreams().isEmpty());
		assertEquals(StreamHandler.getInstance().getRegisteredStreams().size(), 2);
	}
	
	@Test
	public void testDeregisterStream() throws InterruptedException {
		Thread.sleep(300000);
		assertTrue(StreamHandler.getInstance().deregisterStream("https://esa.csiro.au/aus/alertHist?queryType=latest&listTweetIds=true--60000"));
		assertFalse(StreamHandler.getInstance().deregisterStream("https://esa.csiro.au/aus/alertHist?queryType=latest&listTweetIds=true--60000"));
		assertTrue(StreamHandler.getInstance().deregisterStream("http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61--1000"));
		assertFalse(StreamHandler.getInstance().deregisterStream("http://83.145.232.209:10001/?type=vehicles&lng1=23&lat1=60&lng2=26&lat2=61--1000"));
	}


	

}
