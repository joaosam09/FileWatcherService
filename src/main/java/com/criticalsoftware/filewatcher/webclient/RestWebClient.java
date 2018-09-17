package com.criticalsoftware.filewatcher.webclient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestWebClient {

	private final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	
	public RestWebClient() {
		super();
	}
	
	public HttpResponse sendPostRequest(String url, String content) {
		try {		
			URL connectionUrl = new URL(url);
			HttpURLConnection conn = (HttpURLConnection) connectionUrl.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Content-Type", "application/json");
						
			OutputStream os = conn.getOutputStream();
			os.write(content.getBytes());
			os.flush();																	
			
			BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
			
			String output;
			String messageBody = "";
			while ((output = br.readLine()) != null) {
				messageBody += output + "\n";
			}			
			
			HttpResponse response = new HttpResponse(conn.getResponseCode(), messageBody);
			
			conn.disconnect();
			
			return response;
		
		  } catch (MalformedURLException e) {
			  LOGGER.error("Malformed URL: " + e.getMessage());			  
		
		  } catch (IOException e) {
			  LOGGER.error("IOException: " + e.getMessage());
		  }
		
		return new HttpResponse();
	}
}
