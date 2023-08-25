package com.predictspring.dataingestor.SolrInterface;

import java.io.IOException;
import java.math.BigDecimal;

import java.sql.PreparedStatement;

import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.Collection;

import java.util.concurrent.CompletableFuture;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import com.predictspring.dataingestor.DataLoader.DatabaseConnection;

@EnableAsync
public class SolrController {
    
    private static final Logger logger = LoggerFactory.getLogger(SolrController.class);
    
    private static final int FETCH_SIZE = 100;
    private static final int BATCH_SIZE = 200;

    @Async
    public static CompletableFuture<Integer> updateSolr(String preCommitTimeStamp) throws SQLException, SolrServerException, IOException {
        logger.info("Starting Indexing into Solr");

        // This is how we know what has been updated
        String retrieveSql = "SELECT * FROM PRODUCT WHERE LASTUPDATE >= ?";

        SolrConnection s = new SolrConnection();
        s.start();

        DatabaseConnection d = new DatabaseConnection();
        d.start();

        PreparedStatement pstmt = d.getDatabaseConnection().prepareStatement(retrieveSql);

        pstmt.setString(1, preCommitTimeStamp);
        // Specify the numbers of rows we want the database to retrieve
        pstmt.setFetchSize(FETCH_SIZE);

        logger.info("Retrieving data from database");
        ResultSet rs = pstmt.executeQuery();

        int colSize = rs.getMetaData().getColumnCount();
        String[] columnNames = new String[colSize];

        // Get the column names
        for (int i = 0; i < colSize; i++) {
            columnNames[i] = rs.getMetaData().getColumnName(i + 1);
        }
        
        int batchSize = 0;
        Collection<SolrInputDocument> batchs = new ArrayList<SolrInputDocument>();
        
        // TODO: Consider PubSub model for retrieving from database and submitting to solr instance
        while (rs.next()) {
            SolrInputDocument doc = new SolrInputDocument();

            for (int i = 0; i < colSize; i++) {
                String column = columnNames[i];
                
                Object rsValue = rs.getObject(column);
                if (rsValue == null) continue;

                // TODO: Create BigDecimal Field Type in Solr Schema
                if (column.equalsIgnoreCase("Price")) {
                    BigDecimal dec = (BigDecimal) rsValue;
                    doc.addField(column,  dec.doubleValue());
                }
                else {
                    doc.addField(column, rsValue);
                }  
            }

            batchs.add(doc);
            batchSize++;

            // TODO: Commits are expensive, find way to optimize them
            if (batchSize % BATCH_SIZE == 0) {
                s.getSolr().add(batchs);

                s.getSolr().commit();
                
                //Reset the Arraylist
                batchs = new ArrayList<SolrInputDocument>();
                batchSize = 0;
            }
        }

        if (batchSize > 0) {
            s.getSolr().add(batchs);
            s.getSolr().commit();
        }

        pstmt.close();
        d.close();
        s.close();

        logger.info("Completed indexing");

        return CompletableFuture.completedFuture(0);
    }


    public static String getQuery(String myQuery) throws IOException {
        SolrConnection conn = new SolrConnection();

        String result = null;
        try {
            result = (String) conn.returnQuery(myQuery); 
        } catch (SolrServerException e) {
            logger.error(e.getMessage());
        }
        return result;
    }


    public static int deleteAllQuery() throws IOException {
        logger.info("Attempting to delete all entries in Solr");
        SolrConnection s = new SolrConnection();
        s.start();
        s.deleteAllQuery();

        s.close();

        logger.info("Deleted all entries in Solr");
        return 0;
    }
}
