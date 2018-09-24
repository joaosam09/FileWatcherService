package com.criticalsoftware.filewatcher.csv;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.csv.CSVRecord;

import com.criticalsoftware.filewatcher.dbclient.PostgresqlDbClient;
import com.criticalsoftware.filewatcher.webclient.HttpResponse;
import com.criticalsoftware.filewatcher.webclient.OperationResponse;
import com.criticalsoftware.filewatcher.webclient.RestWebClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CsvRecordHandler implements Runnable {
	
	private static final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private final String calculateServiceURL = "http://localhost:8090/calculate";	
	private BlockingQueue<Object> queue;	
	private CsvFileHandler fileHandler;
	private RestWebClient webClient = new RestWebClient();			
	private PostgresqlDbClient dbClient = new PostgresqlDbClient();	
	
	public CsvRecordHandler(CsvFileHandler fileHandler, BlockingQueue<Object> queue) {
        this.queue = queue;          
        this.fileHandler = fileHandler;
    }
	
	@Override
	public void run() {
		try {
            while (true) {
            	Object recordToHandle = queue.take();
            	if(recordToHandle.getClass() == CSVRecord.class) {
            		handleOperationRecord((CSVRecord) recordToHandle);
            	}else {                		                   		
        			return; //JOB TERMINATED    		
            	}                   	
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	}
	
	private void handleOperationRecord(CSVRecord csvRecord) {						
		boolean recordHasError = true;
		
		try {
			double value1 = Double.parseDouble(csvRecord.get("value1"));
	    	double value2 = Double.parseDouble(csvRecord.get("value2"));
	    	String operation = csvRecord.get("operation");	    	
	    	CsvOperationRequest csvRequest = new CsvOperationRequest(value1, value2, operation);		    		    
	    	
	    	ObjectMapper objectMapper = new ObjectMapper();	    	
	    	String jsonRequest = objectMapper.writeValueAsString(csvRequest);
	    	
	    	try {
	    		HttpResponse response = webClient.sendPostRequest(calculateServiceURL, jsonRequest);
	    			    		
	    		if(response.getStatusCode() == HttpURLConnection.HTTP_OK) { 		
		    		OperationResponse operationResponse = objectMapper.readValue(response.getBody(), OperationResponse.class);		    		
		    		double result = operationResponse.getResult();
		    		
		    		//If the file name is too long, use substring to shorten it's length
		    		String fileName = fileHandler.getFileName().length() > 100 ? fileHandler.getFileName().substring(0, 99) : fileHandler.getFileName();
		    				    		
		    		//Writes the values and the result in the database		    		
	    			dbClient.executeUpdate("INSERT INTO operations (value1, value2, result, operation, date_time, file_name) VALUES(?,?,?,?,?,?)",
											value1, value2, result, operation, new java.sql.Timestamp(new Date().getTime()), fileName);		    			    				 
		    		
		    		recordHasError = false;
		    	}
	    	} catch(MalformedURLException e){	    		
	    		LOGGER.info("Malformed URL " + calculateServiceURL);
	    		
	    	} catch(IOException e){	    		
	    		LOGGER.info("Error sending post request: " + e.getMessage());
	    	}	
		
		} catch(NumberFormatException e){    		
    		LOGGER.info("Error parsing double value: " + e.getMessage());
    		
		} catch(JsonProcessingException e){    		
    		LOGGER.error("Error serializing json request: " + e.getMessage());
    			
		} catch(Exception e) {						
			LOGGER.error("Error handling record: " + " value1=" + csvRecord.get("value1")
												   + " value2=" + csvRecord.get("value2")
												   + " operation=" + csvRecord.get("operation") 
												   + " error message=" + e.getMessage());						
		}
		
		if(recordHasError)
			fileHandler.writeRecordInErrorFile(csvRecord);
	}
}
