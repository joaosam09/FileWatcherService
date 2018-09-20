package com.criticalsoftware.filewatcher;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.criticalsoftware.filewatcher.csv.CsvFileWatcher;

/**
 * Class responsible for detecting new files in a directory.
 * The directory to be watched should be passed as an argument
 *
 * @author João Santos
 * @version 1.0
 */
public class MainApp
{
	private static final Logger LOGGER = Logger.getLogger(MainApp.class.toString());
	
    public static void main( String[] args )
    {    	    	
    	if (args.length != 2) {
            System.err.println("Usage: java FileWatcherService <input_folder> <output_folder>");
            System.exit(1);
        }
    	
    	try {
			CsvFileWatcher newFileWatcher = new CsvFileWatcher(args[0], args[1]);
			newFileWatcher.start();
			
    	} catch (IOException e) {			
			LOGGER.log(Level.SEVERE, "Error registering new file watcher: " + e.getMessage());
		}  
    }    
}
