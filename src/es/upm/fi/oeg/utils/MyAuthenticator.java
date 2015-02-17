package es.upm.fi.oeg.utils;

import java.net.Authenticator;
import java.net.InetAddress;
import java.net.PasswordAuthentication;

/*
 * Copied from http://examples.javacodegeeks.com/core-java/net/authenticator/access-password-protected-url-with-authenticator/
 */
public class MyAuthenticator extends Authenticator {
	
    // Called when password authorization is needed
    protected PasswordAuthentication getPasswordAuthentication() {
        // Get information about the request
        String prompt = getRequestingPrompt();
        String hostname = getRequestingHost();
        InetAddress ipaddr = getRequestingSite();
        int port = getRequestingPort();
        
        // ESA credentials
        String username = "allaves";
        String password = "cacafut1";
        
        // Return the information (a data holder that is used by Authenticator)
        return new PasswordAuthentication(username, password.toCharArray());
    }
}
