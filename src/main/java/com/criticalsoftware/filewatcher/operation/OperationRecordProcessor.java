package com.criticalsoftware.filewatcher.operation;

import java.util.concurrent.BlockingQueue;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class OperationRecordProcessor implements Runnable {
	
	private final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private BlockingQueue<CSVRecord> queue;

	public OperationRecordProcessor(BlockingQueue<CSVRecord> queue) {
        this.queue = queue;        
    }
	
	@Override
	public void run() {
		try {
            while (true) {
            	processOperationRecord(queue.take());                
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
	}
	
	private void processOperationRecord(CSVRecord csvRecord) {
		try {
			double value1 = Double.parseDouble(csvRecord.get("value1"));
	    	double value2 = Double.parseDouble(csvRecord.get("value2"));
	    	String operation = csvRecord.get("operation");
	    	
	    	OperationRequest request = new OperationRequest(value1, value2, operation);
	    	LOGGER.info("READ: " + "value1=" + request.getValue1()
	    						 + "value2=" + request.getValue2()
								 + "operation=" + request.getOperation());	    	
	    	
//	    	ObjectMapper objectMapper = new ObjectMapper();
//	    	
//	    	objectMapper.writeValue(response.getWriter(), request);
	    	
		} catch(Exception e) {
			LOGGER.error("Error processing record: value1=" + csvRecord.get("value1")
												+ ";value2=" + csvRecord.get("value2")
												+ ";operation=" + csvRecord.get("operation"));
			//WRITE FAILURE TO INVALID FOLDER
		}				
	}
}
