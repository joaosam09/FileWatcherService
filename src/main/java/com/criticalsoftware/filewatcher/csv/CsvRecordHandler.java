package com.criticalsoftware.filewatcher.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import com.criticalsoftware.filewatcher.webclient.HttpResponse;
import com.criticalsoftware.filewatcher.webclient.OperationResponse;
import com.criticalsoftware.filewatcher.webclient.RestWebClient;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CsvRecordHandler implements Runnable {
	
	private final Logger LOGGER = Logger.getLogger(CsvRecordHandler.class.toString());
	private final String calculateServiceURL = "http://localhost:8090/calculate";	
	private BlockingQueue<Object> queue;
	private String fileName;
	private String outputFolder;
	private RestWebClient webClient = new RestWebClient();	
	private Object fileLock = new Object();
	
	public CsvRecordHandler(BlockingQueue<Object> queue, String fileName, String outputFolder) {
        this.queue = queue;  
        this.fileName = fileName;
        this.outputFolder = outputFolder;
    }
	
	@Override
	public void run() {
		try {
            while (true) {
            	Object recordToHandle = queue.take();
            	if(recordToHandle.getClass() == CSVRecord.class) {
            		handleOperationRecord((CSVRecord) recordToHandle);
            	}else {        			
        			return;        		
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
		    		LOGGER.info("RESULT: " + result);		    		
		    		
		    		//WRITE VALUES IN THE DATABASE
		    		
		    		recordHasError = false;
		    	}
	    	} catch(MalformedURLException e){	    		
	    		LOGGER.info("Malformed URL " + calculateServiceURL);
	    		
	    	} catch(IOException e){	    		
	    		LOGGER.info("Error sending post request: " + e.getMessage());
	    	}	
		
		} catch(NumberFormatException e){    		
    		LOGGER.info("Error sending post request: " + e.getMessage());
    			
		} catch(Exception e) {						
			LOGGER.info("Error handling record from file \"" + fileName + "\":" 
																		+ " value1=" + csvRecord.get("value1")
																		+ " value2=" + csvRecord.get("value2")
																		+ " operation=" + csvRecord.get("operation") 
																		+ " error message=" + e.getMessage());						
		}
		
		if(recordHasError)
			writeRecordInErrorFile(csvRecord);
	}
	
	private void writeRecordInErrorFile(CSVRecord csvRecord) {
		synchronized(fileLock) {					
			try {
				Path errorPath = Paths.get(outputFolder + "\\UnresolvedRequests");						
				if(!Files.exists(errorPath))
					Files.createDirectory(errorPath);
											
				String newFilePath = errorPath + "\\" + fileName;
				
				CSVFormat csvFormat;
				csvFormat = !Files.exists(Paths.get(newFilePath)) ? CSVFormat.DEFAULT.withHeader("value1", "value2", "operation").withDelimiter(';')
																  : CSVFormat.DEFAULT.withDelimiter(';');				
				
				BufferedWriter writer = Files.newBufferedWriter(Paths.get(newFilePath), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
	            CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat);
	            
	            csvPrinter.printRecord(csvRecord.get("value1"), csvRecord.get("value2"), csvRecord.get("operation"));              
	            csvPrinter.close();
	            
	        } catch (IOException e) {
	        	LOGGER.info("Error writing unresolved record on file " + fileName + ":" + e.getMessage());   
			}
		}		
	}
}
