package com.criticalsoftware.filewatcher.csv;

import java.util.concurrent.BlockingQueue;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class CsvRecordHandler implements Runnable {
	
	private final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private BlockingQueue<CsvOperationRequest> queue;

	public CsvRecordHandler(BlockingQueue<CsvOperationRequest> queue) {
        this.queue = queue;        
    }
	
	@Override
	public void run() {
		try {
            while (true) {
            	handleOperationRecord(queue.take());                
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	}
	
	private void handleOperationRecord(CsvOperationRequest csvRequest) {
		if(csvRequest != null) {
			try {
//				double value1 = Double.parseDouble(csvRecord.get("value1"));
//		    	double value2 = Double.parseDouble(csvRecord.get("value2"));
//		    	String operation = csvRecord.get("operation");
//		    	
//		    	CsvOperationRequest request = new CsvOperationRequest(value1, value2, operation);
		    	LOGGER.info("READ: " + "value1=" + csvRequest.getValue1()
		    						 + " value2=" + csvRequest.getValue2()
									 + " operation=" + csvRequest.getOperation());	    	
		    	
//		    	ObjectMapper objectMapper = new ObjectMapper();	    	
//		    	String jsonRequest = objectMapper.writeValueAsString(request);
		    	
			} catch(Exception e) {
				LOGGER.error("Error processing record: value1=" + csvRequest.getValue1()
													+ ";value2=" + csvRequest.getValue2()
													+ ";operation=" + csvRequest.getOperation());
				//WRITE FAILURE TO INVALID FOLDER
			}	
		}else {
			LOGGER.info("TERMINATED JOB");
			return;
		}			
	}
}
