package es.upm.fi.oeg.utils;

import java.util.ArrayList;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientHandlerException;
import com.sun.jersey.api.client.ClientRequest;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.filter.ClientFilter;
import com.sun.jersey.api.representation.Form;

import esa.client.test.ClientHelper;

/*
 * Code provided by Mark Cameron (CSIRO, ACT)
 * Based on http://stackoverflow.com/questions/9676588/how-can-you-authenticate-using-the-jersey-client-against-a-jaas-enabled-web-serv
 */
public class FormBasedAuthentication {

	private Client client;
	private String urlLogin;
	private String urlData;
	private String user;
	private String pass;
	private WebResource webResource;
	
	public FormBasedAuthentication(String urlData, String urlLogin, String user, String password) {
		client = ClientHelper.createClient();
		this.urlLogin = urlLogin;
		this.urlData = urlData;
		this.user = user;
		this.pass = password;
	}
	
	public void authenticate() {
		// add a filter to set cookies received from the server and to check if login has been triggered
		client.addFilter(new ClientFilter() {
			ArrayList<Object> cookies;
	
		    @Override
		    public ClientResponse handle(ClientRequest request) throws ClientHandlerException {
		        if (cookies != null) {
		            request.getHeaders().put("Cookie", cookies);
		        }
		        ClientResponse response = getNext().handle(request);
		        // copy cookies
		        if (response.getCookies() != null) {
		            if (cookies == null) {
		                cookies = new ArrayList<Object>();
		            }
		            // A simple addAll just for illustration (should probably check for duplicates and expired cookies)
		            cookies.addAll(response.getCookies());
		        }
		        return response;
		    }
		});
		
		// Get the protected web page: (this will make the server know that someone	is trying to access the protected resource)
		webResource = client.resource(this.urlData);
		webResource.get(String.class);

		// Login:
		webResource = client.resource(this.urlLogin);
		
		// Create form and submit it via post
		com.sun.jersey.api.representation.Form form = new Form();
		form.putSingle("j_username", this.user);
		form.putSingle("j_password", this.pass);
		webResource.type("application/x-www-form-urlencoded").post(form);
	}
	
	public String getData() {
		// Get the protected web page: (this time the service will return the data)
		webResource = client.resource(this.urlData);
		return webResource.get(String.class);
	}
	
}
