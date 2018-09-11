package com.criticalsoftware.filewatcher.file;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.criticalsoftware.filewatcher.operation.OperationRecordProcessor;

public class FileProcessor {

	private final static Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private static final int QUEUECAPACITY = 1000;
	private static BlockingQueue<CSVRecord> waitingQueue = new LinkedBlockingQueue<>(QUEUECAPACITY);
	private static int NR_THREADS = Runtime.getRuntime().availableProcessors();
	
	/**
	 * Private to not allow instantiation of the class.
	 */
	private FileProcessor() {};
	
	public static void processFile(Path filePath) throws IOException {
		BufferedReader reader = Files.newBufferedReader(filePath);
	    CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT.withHeader("value1", "value2", "operation")
	    									.withIgnoreHeaderCase().withTrim());
	    
	    for (CSVRecord csvRecord: csvParser) {
	    	try {
				waitingQueue.put(csvRecord);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOGGER.error("Thread interrupted while populating queue: " + e.getMessage());
			}	    	    	                   
	    }
	    
	    for (int j = 0; j < NR_THREADS; j++) {
	        new Thread(new OperationRecordProcessor(waitingQueue)).start();
	    }
	    
	    csvParser.close();	     	    
	}	
}
