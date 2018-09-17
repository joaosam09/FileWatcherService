package com.criticalsoftware.filewatcher.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvFileHandler implements Runnable {

	private final static Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private final int QUEUECAPACITY = 1000;
	private BlockingQueue<Object> waitingQueue = new LinkedBlockingQueue<>(QUEUECAPACITY);
	private int NR_THREADS = Runtime.getRuntime().availableProcessors();
	private Path filePath;
	private String outputFolder;
	
	public CsvFileHandler(Path filePath, String outputFolder) {
        this.filePath = filePath;  
        this.outputFolder = outputFolder;
    }
	
	@Override
	public void run() {
		BufferedReader reader;
		CSVParser csvParser;
		
		try {
			reader = Files.newBufferedReader(filePath);
			csvParser = new CSVParser(reader, CSVFormat.DEFAULT											
											  .withHeader("value1", "value2", "operation")
											  .withSkipHeaderRecord()                							
											  .withIgnoreHeaderCase()
											  .withDelimiter(';')
											  .withTrim());
			
			String fileName = filePath.getFileName().toString();
			
			//Starts the record handler threads (they will wait until the queue has records)
		    for (int j = 0; j < NR_THREADS; j++) {	    	
		        Thread newRecordHandlerThread = new Thread(new CsvRecordHandler(waitingQueue, fileName, outputFolder));
		        newRecordHandlerThread.start();
		        
//		        try {
//					newRecordHandlerThread.join();
//				} catch (InterruptedException e) {
//					LOGGER.error("Thread interrupted while handling records: " + e.getMessage());
//					Thread.currentThread().interrupt();				
//				}
		    }	
		    
		    //Starts inserting records into the queue
			for (Object csvRecord : csvParser) {			
		    	try {
					waitingQueue.put(csvRecord);
				} catch (InterruptedException e) {
					LOGGER.error("Thread interrupted while populating queue: " + e.getMessage());
					Thread.currentThread().interrupt();					
				}	    	    	                   
		    }
		    
		    //For each thread, inserts an empty record to finish execution gracefully
		    for (int i = 0; i < NR_THREADS; i++) {
		    	try {
					waitingQueue.put(new Object());
				} catch (InterruptedException e) {
					LOGGER.error("Thread interrupted while inserting task ending records: " + e.getMessage());
					Thread.currentThread().interrupt();				
				};
		    }		    	        	    
		    
		    csvParser.close();
		    reader.close();	
		    
		} catch (IOException e) {
			LOGGER.error("Error handling file \"" + filePath + "\": " + e.getMessage());					
		}									
	}		
}
