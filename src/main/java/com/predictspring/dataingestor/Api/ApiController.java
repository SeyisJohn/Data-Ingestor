package com.predictspring.dataingestor.Api;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import java.text.SimpleDateFormat;

import org.apache.solr.client.solrj.SolrServerException;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PostMapping;

import org.springframework.web.multipart.MultipartFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.predictspring.dataingestor.DataLoader.DatabaseController;
import com.predictspring.dataingestor.SolrInterface.SolrController;


@RestController
@RequestMapping(path="/api")
public class ApiController {
    /*
     * TODO: Don't use static and consider using none
     * Consider using more flexibility
     */
    private static final Logger logger = LoggerFactory.getLogger(ApiController.class);

    @PostMapping(value="/data")
    public ResponseEntity<String> uploadFile(@RequestParam("file") MultipartFile multipartFile) {
        
        long startTime = System.currentTimeMillis();
        Timestamp startDataIngestion = new Timestamp(startTime);
        logger.info("Ingesting data at: " + startDataIngestion.toString());

        try {
            DatabaseController.handleFileUpload(multipartFile);
        } 
        catch (SQLException | IOException e) {
            logger.error(e.getMessage());

            return new ResponseEntity<String>("System could not properly store file", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        // Data ingestion is too fast for nano seconds, so we will need to chop it off
        // Convert to 24 hour format
        SimpleDateFormat formattedTimeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // Async call to start indexing
        CompletableFuture.supplyAsync(() -> {
            try {
                SolrController.updateSolr(formattedTimeStamp.format(startDataIngestion));
            } catch (SQLException | SolrServerException | IOException e) {
                logger.error("Indexed failed for data ingestor at: " + startDataIngestion.toString());
                logger.error("Indexing failure error message: " + e.getMessage());
            }
            return 0;
        }).orTimeout(60, TimeUnit.SECONDS);

        return new ResponseEntity<String>("File was properly uploaded", HttpStatus.OK);
    }
    
    
    @GetMapping(value="/data")
    public ResponseEntity<String> getQuery(@RequestParam(value="q", defaultValue="*:*") String q) {
        String response;

        try {
            response = SolrController.getQuery(q);
        } 
        catch (Exception e) {
            return new ResponseEntity<String>("Query is invalid", HttpStatus.INTERNAL_SERVER_ERROR);
        }
        
        if (response == null) {
            return new ResponseEntity<String>("Query not found", HttpStatus.OK);
        }

        return new ResponseEntity<String>(response, HttpStatus.OK);
    }

    // TODO: Remove this after debugging not safe in production
    @DeleteMapping(value="/data/")
    public ResponseEntity<String> deleteAll() {
        try {
            DatabaseController.deleteAll();
            SolrController.deleteAllQuery();
        } catch (SQLException | IOException e) {
            logger.error(e.getMessage());
            return new ResponseEntity<String>("System could not properly delete data", HttpStatus.INTERNAL_SERVER_ERROR);
        }

        return new ResponseEntity<String>("Data within Database and Solr Instance was deleted", HttpStatus.OK);
    }
    
}
