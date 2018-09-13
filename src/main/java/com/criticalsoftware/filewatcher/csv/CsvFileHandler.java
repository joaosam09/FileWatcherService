package com.criticalsoftware.filewatcher.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;

public class CsvFileHandler {

	private final static Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private static final int QUEUECAPACITY = 1000;
	private static BlockingQueue<CsvOperationRequest> waitingQueue = new LinkedBlockingQueue<>(QUEUECAPACITY);
	private static int NR_THREADS = Runtime.getRuntime().availableProcessors();
	
	/**
	 * Private to not allow instantiation of the class.
	 */
	private CsvFileHandler() {};
	
	public static void processFile(Path filePath) throws IOException {
		BufferedReader reader = Files.newBufferedReader(filePath);
		CsvToBean<CsvOperationRequest> csvToBean = new CsvToBeanBuilder<CsvOperationRequest>(reader)   
													   .withSeparator(';')                									   
                									   .withType(CsvOperationRequest.class)
                									   .build();
		
		Iterator<CsvOperationRequest> csvIterator = csvToBean.iterator();
		
		while (csvIterator.hasNext()) {			
	    	try {
				waitingQueue.put(csvIterator.next());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				LOGGER.error("Thread interrupted while populating queue: " + e.getMessage());
			} catch (Exception e) {				
				LOGGER.error("Invalid operation request info: " + e.getMessage());
			}	    	    	                   
	    }
	    
	    //For each thread, inserts a null record to finish execution gracefully
//	    for (int i = 0; i < NR_THREADS; i++) {
//	    	try {
//				waitingQueue.put(new CsvOperationRequest());
//			} catch (InterruptedException e) {
//				Thread.currentThread().interrupt();
//				LOGGER.error("Thread interrupted while inserting poison pills: " + e.getMessage());
//			};
//	    }
	    
	    for (int j = 0; j < NR_THREADS; j++) {
	        new Thread(new CsvRecordHandler(waitingQueue)).start();
	    }
	    
	    reader.close();
	}	
}
