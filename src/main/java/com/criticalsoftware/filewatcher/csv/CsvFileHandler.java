package com.criticalsoftware.filewatcher.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

public class CsvFileHandler implements Runnable {

	private static final Logger LOGGER = Logger.getLogger(CsvFileHandler.class.toString());
	private final int QUEUECAPACITY = 1000;
	private int NR_THREADS = Runtime.getRuntime().availableProcessors();
	private BlockingQueue<Object> waitingQueue = new LinkedBlockingQueue<>(QUEUECAPACITY);	
	private Path filePath;
	private String outputFolder;
	
	public CsvFileHandler(Path filePath, String outputFolder) {
        this.filePath = filePath;  
        this.outputFolder = outputFolder;
    }
	
	@Override
	public void run() {		
		try {
			BufferedReader reader = Files.newBufferedReader(filePath);
			CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT											
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
		    }	
		    
		    //Starts inserting records into the queue
			for (Object csvRecord : csvParser) {			
		    	try {
					waitingQueue.put(csvRecord);
				} catch (InterruptedException e) {
					LOGGER.log(Level.SEVERE, "Thread interrupted while populating queue: " + e.getMessage());
					Thread.currentThread().interrupt();					
				}	    	    	                   
		    }
		    
		    //For each thread, inserts an empty record to finish execution gracefully
		    for (int i = 0; i < NR_THREADS; i++) {
		    	try {
					waitingQueue.put(new Object());
				} catch (InterruptedException e) {
					LOGGER.log(Level.SEVERE, "Thread interrupted while inserting task ending records: " + e.getMessage());
					Thread.currentThread().interrupt();				
				};
		    }		    	        	    
		    
		    csvParser.close();
		    reader.close();
		    
		    moveFileToOutputDirectory();
		    
		} catch (IOException e) {
			LOGGER.log(Level.SEVERE, "Error handling file \"" + filePath + "\": " + e.getMessage());					
		}									
	}
	
	private void moveFileToOutputDirectory() {								
		try {
			Files.move(filePath, Paths.get(outputFolder + "\\" + filePath.getFileName().toString()), StandardCopyOption.REPLACE_EXISTING);            
        } catch (IOException e) {
        	LOGGER.log(Level.SEVERE, "Error moving file " + filePath.getFileName().toString() + " to output directory: " + e.getMessage());   
		}
		
	}
}
