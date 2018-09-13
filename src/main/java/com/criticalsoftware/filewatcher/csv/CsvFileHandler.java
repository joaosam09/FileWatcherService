package com.criticalsoftware.filewatcher.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CsvFileHandler {

	private final static Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private static final int QUEUECAPACITY = 1000;
	private static BlockingQueue<Object> waitingQueue = new LinkedBlockingQueue<>(QUEUECAPACITY);
	private static int NR_THREADS = Runtime.getRuntime().availableProcessors();
	
	/**
	 * Private to not allow instantiation of the class.
	 */
	private CsvFileHandler() {};
	
	public static void processFile(Path filePath) throws IOException {
		BufferedReader reader = Files.newBufferedReader(filePath);			
		CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT											
                							.withHeader("value1", "value2", "operation")
                							.withSkipHeaderRecord()                							
                							.withIgnoreHeaderCase()
                							.withDelimiter(';')
                							.withTrim());
				
		for (Object csvRecord : csvParser) {			
	    	try {
				waitingQueue.put(csvRecord);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOGGER.error("Thread interrupted while populating queue: " + e.getMessage());			
			}	    	    	                   
	    }
	    
	    //For each thread, inserts a null record to finish execution gracefully
	    for (int i = 0; i < NR_THREADS; i++) {
	    	try {
				waitingQueue.put(new Object());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOGGER.error("Thread interrupted while inserting null records: " + e.getMessage());
			};
	    }
	    
	    //Starts the record handler threads
	    for (int j = 0; j < NR_THREADS; j++) {
	        new Thread(new CsvRecordHandler(waitingQueue)).start();
	    }
	    
	    csvParser.close();
	    reader.close();
	}	
}
