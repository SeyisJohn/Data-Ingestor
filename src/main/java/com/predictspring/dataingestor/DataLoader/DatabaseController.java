package com.predictspring.dataingestor.DataLoader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.springframework.web.multipart.MultipartFile;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DatabaseController {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseController.class);

    // Specifys the batch size of the commands
    private static final int BLOCKSIZE = 20;

    // Consider using the Java Constructors
    public static int handleFileUpload(MultipartFile file) throws IOException, SQLException {
        logger.info("Uploading file to database");
        String fileName = file.getOriginalFilename();

        String fileExtension = checkFileType(fileName);

        // Consider using Apacpe Tikka instead to properly find file type
        if (fileExtension == null) {
            logger.error("File extension was not specified");
            return 1;
        }

        char delimiter = returnDelimiter(fileExtension);

        if (delimiter == 1) {
            logger.error("Parser is not compatiable with file type");
            return 1;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream(), "UTF-8"));
        
        DatabaseConnection database = new DatabaseConnection();

        database.start();

        // Retreive the column names
        String line = "";
        String[] columnNames = null;
        int validCols = 0;
        line = reader.readLine();

        if (line != null) {
            columnNames = parseLine(line, delimiter);

            for (int i = 0; i < columnNames.length; i++) {
                String columnName = columnNames[i];

                // If the columnName is not in our database schema
                //  and if its mapping does not exist, we should ignore it
                if (Product.mappingExist(columnName)) {
                    columnNames[i] = Product.getMapping(columnName);
                    validCols++;
                }
                else if (!Product.contains(columnName)) {
                    columnNames[i] = null;
                }
                else {
                    validCols++;
                }

                logger.info("Column " + i + " is: " + columnNames[i]);
            }
        }

        // Keeps track of how many commands we are adding to batch
        int blocks = 0;

        String[] record;

        // Because we are ignoring some columns, we need a way to proper count the index
        int columnIndex = 1;

        // Consider the using JPA instead of jdbc
        String Sql = Product.generateInsertSQL(columnNames);

        if (Sql == null) {
            logger.error("File does not have columns headers");
            return 1;
        }
        
        // Consider using JPA instead JDBC
        PreparedStatement pstmt = database.getDatabaseConnection().prepareStatement(Sql);

        // Parsing the rest of the file
        while ((line = reader.readLine()) != null) {
            //logger.info("Line: " + line);

            record = parseLine(line, delimiter);
            
            // Filling in the placeholder in the SQL STATEMENT
            for (int i = 0; i < columnNames.length; i++) {

                if (columnNames[i] == null) continue;
                
                if (columnNames[i].equalsIgnoreCase("Name")) {
                    logger.info("Name: " + record[i]);
                }

                // FOR INSERT Part in SQL STATEMENT
                pstmt.setString(columnIndex, record[i]);

                // FOR DUPLICATE Part in SQL STATEMENT
                pstmt.setString(validCols + columnIndex, record[i]);

                columnIndex++;
            }

            // Reset counter
            columnIndex = 1;

            // Adds Sql Command to a batch
            pstmt.addBatch();
            blocks++;

            // Once we reach our block size, we execute the system
            if (blocks % BLOCKSIZE == 0) {
                pstmt.executeBatch();
                blocks = 0;
            }
        }

        // Execute batch if they are still some commands left after finishing reading file
        if (blocks > 0) {
            pstmt.executeBatch();
        }

        // Make sure the cleanup process is correct
        // Close this if an issue
        reader.close();
        database.close();

        return 0;
    }

    private static String checkFileType(String fileName) {
        int len = fileName.length();
        logger.info("File name: " + fileName);

        String fileExtension = "";
        int j = len - 1;
        while (j >= 0 && fileName.charAt(j) != '.') j--;

        if (j < 0) {
            logger.info("File extension was not specified");
            return null;
        }

        // Plus one to not include the dot
        fileExtension = fileName.substring(j + 1, len);

        return fileExtension;
    }


    private static char returnDelimiter(String fileExtension) {

        if (fileExtension.equals("csv")) return ',';

        else if (fileExtension.equals("tsv")) return '\t';
        
        return 1;
    }


    // This solves the main issue when we are parsing by a delimiter
    //  and there is exist a delimiter inside a quoted phrase
    // Quotes are not included in the result
    private static String[] parseLine(String line, char delimiter) {
        
        if (delimiter == '\t') {
            String[] result = line.split("\t");

            for(int i = 0; i < result.length; i++) {
                result[i] = result[i].trim();
            }

            return result;
        }

        ArrayList<String> result = new ArrayList<String>();
        
        boolean insideQuotes = false;
        StringBuilder currentWord = new StringBuilder();

        //Escaping the quotes
        // char[] charLine = line.replace("\"", "\"\"").toCharArray();
        char[] charLine = line.toCharArray();
        for (int i = 0; i < charLine.length; i++) {
            char c = charLine[i];
            if (c == '\"') {
                insideQuotes = !insideQuotes;
                continue;
            }

            if (c == delimiter && !insideQuotes) {
                result.add(currentWord.toString().trim());
                currentWord.setLength(0); // Reset the builder for the next word.
            } 
            else {
                currentWord.append(c);
            }
        }

        result.add(currentWord.toString().trim()); // Add the last word

        return result.toArray(new String[result.size()]);
    }

    public static void deleteAll() throws SQLException {
        String SQL = "DELETE FROM PRODUCT";
        logger.info("Deleting all data in database");
        DatabaseConnection database = new DatabaseConnection();

        
        database.start();
        PreparedStatement stmt = database.getDatabaseConnection().prepareStatement(SQL);
        stmt.execute();
        database.close();

        logger.info("Deleted all data in database");
    }
}