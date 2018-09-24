package com.criticalsoftware.filewatcher.dbclient;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.dbcp2.BasicDataSource;

/**
 * A simple PostgreSql database client
 *
 * @author João Santos
 * @version 1.0
 */
public class PostgresqlDbClient {
	
	private static final Logger LOGGER = LoggerFactory.getLogger("ApplicationFileLogger");
    private static BasicDataSource dataSource;
        
    private static BasicDataSource getDataSource()
    {
        if (dataSource == null)
        {
            BasicDataSource basicDataSource = new BasicDataSource();            
            basicDataSource.setUrl("jdbc:postgresql://localhost/file_watcher_db");
            basicDataSource.setUsername("postgres");
            basicDataSource.setPassword("critical");
//            basicDataSource.setMinIdle(5);
//            basicDataSource.setMaxIdle(10);
//            basicDataSource.setMaxOpenPreparedStatements(100);       
            
            dataSource = basicDataSource;
        }
        return dataSource;
    }
        
    /**
     * Connect to the PostgreSQL database
     *
     * @return Connection The database connection
     */
    private Connection getConnection() {
        Connection conn = null;
        try {              	        	        	
            conn = getDataSource().getConnection();            
        } catch (SQLException e) {
            LOGGER.error("Error getting database connection: " + e.getMessage());
        }
        return conn;
    }       
        
    /**
     * Execute an update/create/delete in the database using a prepared statement
     *
     * @return Connection The database connection
     */
    public void executeUpdate(String query, Object... parameters) throws SQLException {
    	Connection connection = getConnection();
    	
    	if(connection != null) {
    		PreparedStatement newStatement = null;
    		
    		try {    			
    			newStatement = connection.prepareStatement(query);				
    			for(int i = 0; i < parameters.length; i++) {
					newStatement.setObject(i+1, parameters[i]);
				}				
    			newStatement.executeUpdate();
				    			
			} catch (SQLException e) {
				LOGGER.error("Error executing statement \"" + query + "\": " + e.getMessage());
				
			} finally {
    			if (newStatement != null)
    				newStatement.close();
    			
    			connection.close();
			}
    	}    	
    }
	
}
