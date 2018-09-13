package com.criticalsoftware.filewatcher.csv;

import java.util.concurrent.BlockingQueue;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CsvRecordHandler implements Runnable {
	
	private final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private BlockingQueue<Object> queue;

	public CsvRecordHandler(BlockingQueue<Object> queue) {
        this.queue = queue;        
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
		try {				
			double value1 = Double.parseDouble(csvRecord.get("value1"));
	    	double value2 = Double.parseDouble(csvRecord.get("value2"));
	    	String operation = csvRecord.get("operation");
	    	
	    	CsvOperationRequest csvRequest = new CsvOperationRequest(value1, value2, operation);
	    	LOGGER.info("READ: " + "value1=" + csvRequest.getValue1()
	    						 + " value2=" + csvRequest.getValue2()
								 + " operation=" + csvRequest.getOperation());	    	
	    	
	    	ObjectMapper objectMapper = new ObjectMapper();	    	
	    	String jsonRequest = objectMapper.writeValueAsString(csvRequest);
	    	
		} catch(Exception e) {
			LOGGER.error("Error processing record: value1=" + csvRecord.get("value1")
												+ ";value2=" + csvRecord.get("value2")
												+ ";operation=" + csvRecord.get("operation"));
			//WRITE FAILURE TO INVALID FOLDER
		}
	}
}
