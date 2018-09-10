package com.criticalsoftware.filewatcher.file;

import java.io.IOException;
import java.nio.file.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible for detecting new files in a directory.
 * The directory to be watched should be passed as an argument
 *
 * @author João Santos
 * @version 1.0
 */
public class FileWatcher 
{
	//Logger declared in resources/logback.xml
	private final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private WatchService watchService;
	
    public FileWatcher(String directoryToWatch) throws IOException
    {    	    	  
		watchService = FileSystems.getDefault().newWatchService();
		Path path = Paths.get(System.getProperty(directoryToWatch));
		path.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);						        				    
    }
    
    /**
     * Start the watch service        
     */
    public void start() {
        try {
        	WatchKey key;
			while ((key = watchService.take()) != null) {
			    for (WatchEvent<?> event : key.pollEvents()) {
			        LOGGER.info("New file detected: " + event.context());	        
			        
			    }
			    key.reset();
			}
		} catch (InterruptedException e) {
			LOGGER.error("Watch service interrupted: " + e.getMessage());
		}
    }
}
