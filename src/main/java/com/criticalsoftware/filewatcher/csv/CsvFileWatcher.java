package com.criticalsoftware.filewatcher.csv;

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
public class CsvFileWatcher 
{	
	private final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	private WatchService watchService;
	private String outputFolder;
	
    public CsvFileWatcher(String inputFolder, String outputFolder) throws IOException
    {    	    	      	    	
    	this.outputFolder = outputFolder;
    	
		watchService = FileSystems.getDefault().newWatchService();
		Path path = Paths.get(inputFolder);
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
    			        
			        	Thread newFileHandlerThread = new Thread( new CsvFileHandler(fullPath, outputFolder));
			        	newFileHandlerThread.start();						
			    	}			        
			    }
			    key.reset();
			}
		} catch (InterruptedException e) {
			LOGGER.error("Watch service interrupted: " + e.getMessage());
		}
    }    
}
