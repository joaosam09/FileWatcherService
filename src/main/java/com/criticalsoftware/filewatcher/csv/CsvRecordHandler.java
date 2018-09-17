package com.criticalsoftware.filewatcher.csv;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.criticalsoftware.filewatcher.webclient.HttpResponse;
import com.criticalsoftware.filewatcher.webclient.OperationResponse;
import com.criticalsoftware.filewatcher.webclient.RestWebClient;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.HttpURLConnection;

public class CsvRecordHandler implements Runnable {
	
	private final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private final String calculateServiceURL = "http://localhost:8090/calculate";
	private BlockingQueue<Object> queue;
	private String fileName;
	private String outputFolder;
	private RestWebClient webClient = new RestWebClient();	

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
        			LOGGER.info("TERMINATED JOB");
        			return;        		
            	}                   	
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	}
	
	private void handleOperationRecord(CSVRecord csvRecord) {
		boolean recordHasError = false;
		
		try {				
			double value1 = Double.parseDouble(csvRecord.get("value1"));
	    	double value2 = Double.parseDouble(csvRecord.get("value2"));
	    	String operation = csvRecord.get("operation");
	    	
	    	CsvOperationRequest csvRequest = new CsvOperationRequest(value1, value2, operation);	    		    		    	
	    	ObjectMapper objectMapper = new ObjectMapper();	    	
	    	String jsonRequest = objectMapper.writeValueAsString(csvRequest);	    	
	    	HttpResponse response = webClient.sendPostRequest(calculateServiceURL, jsonRequest);
	    	
	    	if(response.getStatusCode() != HttpURLConnection.HTTP_OK) {
	    		recordHasError = true;
	    	} else {
	    		OperationResponse operationResponse = objectMapper.readValue(response.getBody(), OperationResponse.class);
	    		
	    		double result = operationResponse.getResult();
	    		LOGGER.info("RESULT: " + result);
	    		//WRITE VALUES IN THE DATABASE
	    	}	
	    	
		} catch(Exception e) {
			recordHasError = true;
			LOGGER.error("Error handling record from file \"" + fileName + "\":"
												+ "	value1=" + csvRecord.get("value1")
												+ " value2=" + csvRecord.get("value2")
												+ " operation=" + csvRecord.get("operation") 
												+ " error message=" + e.getMessage());						
		}
		
		if(recordHasError)
			writeRecordInErrorFile(csvRecord);
	}
	
	private void writeRecordInErrorFile(CSVRecord csvRecord) {
		String errorFileName = fileName.split("\\.")[0] + "_Unresolved.csv";
		
		try {
			Path errorPath = Paths.get(outputFolder + "\\UnresolvedRequests");						
			if(!Files.exists(errorPath))
				Files.createDirectory(errorPath);
										
			String newFilePath = errorPath + "\\" + errorFileName;
			BufferedWriter writer = Files.newBufferedWriter(Paths.get(newFilePath));
            CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
		                    							   .withHeader("value1", "value2", "operation")
		                    							   .withDelimiter(';'));
            
            csvPrinter.printRecord(csvRecord.get("value1"), csvRecord.get("value2"), csvRecord.get("operation"));              
            csvPrinter.close();
            
        } catch (IOException e) {
        	LOGGER.error("Error writing unresolved record on file " + errorFileName + ":" + e.getMessage());   
		}
	}
}
