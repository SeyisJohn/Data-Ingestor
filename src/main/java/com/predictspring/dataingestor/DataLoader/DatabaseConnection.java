package com.predictspring.dataingestor.DataLoader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseConnection {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseConnection.class);

    private static final String DATABASE_URL = "jdbc:mysql://localhost:3306/predictspring";
    private static final String DATABASE_USER = "root";
    private static final String DATABASE_PASSWORD = "12345678";

    // Connection to access Database
    private Connection conn;

    public int start() throws SQLException {
        logger.info("Attempting to connect to Database: " + DATABASE_URL);
        
        this.conn = DriverManager.getConnection(DATABASE_URL, DATABASE_USER, DATABASE_PASSWORD);

        logger.info("Connected to Database: " + DATABASE_URL);
        return 0;
    }

    public int close() throws SQLException {
        logger.info("Attempting to close connection to Database: " + DATABASE_URL);

        this.conn.close();
        
        logger.info("Closed connection to Database: " + DATABASE_URL);
        return 0;
    }

    public Connection getDatabaseConnection() {
        return this.conn;
    }
}
