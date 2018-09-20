package com.criticalsoftware.filewatcher.csv;

import java.io.IOException;
import java.nio.file.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Class responsible for detecting new files in a directory.
 * The directory to be watched should be passed as an argument
 *
 * @author João Santos
 * @version 1.0
 */
public class CsvFileWatcher 
{	
	private final Logger LOGGER = Logger.getLogger(CsvFileWatcher.class.toString());
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
        	LOGGER.log(Level.INFO, "File watcher started...");
        	
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
			LOGGER.log(Level.SEVERE, "Watch service interrupted: " + e.getMessage());
		}
    }    
}
