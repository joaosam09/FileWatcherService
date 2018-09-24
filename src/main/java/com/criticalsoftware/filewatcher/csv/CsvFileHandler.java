package com.criticalsoftware.filewatcher.csv;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class CsvFileHandler implements Runnable {

	private static final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private final int QUEUECAPACITY = 100;
	private int NR_THREADS = Runtime.getRuntime().availableProcessors();
	private BlockingQueue<Object> waitingQueue = new LinkedBlockingQueue<>(QUEUECAPACITY);	
	private Path filePath;
	private String fileName;
	private String outputFolder;
	
	public CsvFileHandler(Path filePath, String outputFolder) {
        this.filePath = filePath;  
        this.fileName = filePath.getFileName().toString();
        this.outputFolder = outputFolder;
    }
	
	public String getFileName() {
		return fileName;
	}

	@Override
	public void run() {		
		BufferedReader reader = null;
		CSVParser csvParser = null;
		
		try {
			reader = Files.newBufferedReader(filePath);
			csvParser = new CSVParser(reader, CSVFormat.DEFAULT											
											  .withHeader("value1", "value2", "operation")
											  .withSkipHeaderRecord()                							
											  .withIgnoreHeaderCase()
											  .withDelimiter(';')
											  .withTrim());
						
			//Starts the record handler threads (they will wait until the queue has records)
		    for (int j = 0; j < NR_THREADS; j++) {	    	
		        Thread newRecordHandlerThread = new Thread(new CsvRecordHandler(this, waitingQueue));
		        newRecordHandlerThread.start();		        
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
		    
		    moveFileToOutputDirectory();
		    
		} catch (IOException e) {
			LOGGER.error("Error handling file \"" + filePath + "\": " + e.getMessage());			
			
		} finally {
			try {
				if(csvParser != null)
					csvParser.close();
				
				if(reader != null)
					reader.close();
				
			} catch (IOException e) {
				LOGGER.error("Error closing file resources: " + e.getMessage());
			}		    
		}									
	}
	
	private void moveFileToOutputDirectory() {								
		try {
			Files.move(filePath, Paths.get(outputFolder + "\\" + fileName), StandardCopyOption.REPLACE_EXISTING);            
        } catch (IOException e) {
        	LOGGER.error("Error moving file " + fileName + " to output directory: " + e.getMessage());   
		}		
	}
	
	public synchronized void writeRecordInErrorFile(CSVRecord csvRecord) {		
		CSVPrinter csvPrinter = null;
		
		try {
			Path errorPath = Paths.get(outputFolder + "\\UnresolvedRequests");						
			if(!Files.exists(errorPath))
				Files.createDirectory(errorPath);
												
			String newFilePath = errorPath + "\\" + fileName;
			
			CSVFormat csvFormat;
			csvFormat = !Files.exists(Paths.get(newFilePath)) ? CSVFormat.DEFAULT.withHeader("value1", "value2", "operation").withDelimiter(';')
															  : CSVFormat.DEFAULT.withDelimiter(';');				
			
			BufferedWriter writer = Files.newBufferedWriter(Paths.get(newFilePath), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            csvPrinter = new CSVPrinter(writer, csvFormat);
            
            csvPrinter.printRecord(csvRecord.get("value1"), csvRecord.get("value2"), csvRecord.get("operation"));                          
            
        } catch (IOException e) {
        	LOGGER.error("Error writing unresolved record on file " + fileName + ":" + e.getMessage());   
        	
        } finally {
        	if(csvPrinter != null) {
        		try {
					csvPrinter.close();
				} catch (IOException e) {					
					LOGGER.error("Error closing unresolved records file: " + e.getMessage());
				}
        	}				
		}			
	}
}
