package com.criticalsoftware.filewatcher;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.criticalsoftware.filewatcher.file.FileWatcher;

/**
 * Class responsible for detecting new files in a directory.
 * The directory to be watched should be passed as an argument
 *
 * @author João Santos
 * @version 1.0
 */
public class MainApp 
{
	//Logger declared in resources/logback.xml
	private static final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
	
    public static void main( String[] args )
    {    	    	
    	if (args.length != 1) {
            System.err.println("Usage: java FileWatcherService <directory_to_watch>");
            System.exit(1);
        }
    	
    	try {
			FileWatcher newFileWatcher = new FileWatcher(args[0]);
			
			if(newFileWatcher != null)
				newFileWatcher.start();
			
    	} catch (IOException e) {			
			LOGGER.error("Error registering new file watcher: " + e.getMessage());
		}  
    }    
}
