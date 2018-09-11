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
	private final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private WatchService watchService;
		
    public FileWatcher(String directoryToWatch) throws IOException
    {    	    	      	    	
		watchService = FileSystems.getDefault().newWatchService();
		Path path = Paths.get(directoryToWatch);
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
			    	if(event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
			    		Path dir = (Path)key.watchable();
			    		Path fullPath = dir.resolve((Path) event.context());
			    		
			    		LOGGER.info("New file detected: " + fullPath);
    			        
				        try {
							FileProcessor.processFile(fullPath);
						} catch (IOException e) {
							LOGGER.error("Error processing file " + event.context());
						}
			    	}			        
			    }
			    key.reset();
			}
		} catch (InterruptedException e) {
			LOGGER.error("Watch service interrupted: " + e.getMessage());
		}
    }    
}
