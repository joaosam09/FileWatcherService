package com.criticalsoftware.filewatcher.csv;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class responsible for detecting new files in a directory.
 * The directory to be watched should be passed as an argument as well as the output directory where the handled files should be moved to.
 *
 * @author João Santos
 * @version 1.0
 */
public class CsvFileWatcher 
{	
	private static final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");

	private WatchService watchService;
	private String inputFolder;
	private String outputFolder;
	
    public CsvFileWatcher(String inputFolder, String outputFolder) throws IOException
    {    	    	      	    	
    	Path inputPath = Paths.get(inputFolder);						
		if(!Files.exists(inputPath))
			throw new IOException("Non-existent input directory " + inputFolder + ".");
		
		Path outputPath = Paths.get(outputFolder);						
		if(!Files.exists(outputPath))
			throw new IOException("Non-existent output directory " + outputFolder + ".");
    	
		this.inputFolder = inputFolder;
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
        	LOGGER.info("File watcher started...");
        	
        	handleExistingFiles();
        	
        	WatchKey key;
			while ((key = watchService.take()) != null) {
			    for (WatchEvent<?> event : key.pollEvents()) {
			    	if(event.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
			    		Path dir = (Path)key.watchable();
			    		Path fullPath = dir.resolve((Path) event.context());
			    		
			    		LOGGER.info("New file detected: " + fullPath);
    			        
			        	Thread newFileHandlerThread = new Thread(new CsvFileHandler(fullPath, outputFolder));
			        	newFileHandlerThread.start();		
			    	}			        
			    }
			    key.reset();
			}
		} catch (InterruptedException e) {
			LOGGER.error("Watch service interrupted: " + e.getMessage());
		}
    }    
    
    /**
     * Handles the existing files in the input directory.
     */
    public void handleExistingFiles() {       	    	     
    	File dir = new File(inputFolder);        	  
	    for (File existingFile : dir.listFiles()) {
	    	LOGGER.info("Handling file: " + existingFile.getPath());
	    	
	    	Thread newFileHandlerThread = new Thread(new CsvFileHandler(existingFile.toPath(), outputFolder));
        	newFileHandlerThread.start();	
	    }        	    
    }   
}
