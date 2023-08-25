package com.predictspring.dataingestor.SolrInterface;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SolrConnection {

    private static final Logger logger = LoggerFactory.getLogger(SolrConnection.class);

    private final String SOLR_URL = "http://localhost:8983/solr/predictspring";
    
    private SolrClient solr;

    public SolrClient getSolr() {
        return this.solr;
    }

    public int start() {
        logger.info("Attempting to connect to Solr Instance: " + SOLR_URL);

        this.solr = new Http2SolrClient.Builder(SOLR_URL).build();

        logger.info("Connected to Solr Instance: " + SOLR_URL);
        return 0;
    }

    public int close() throws IOException {
        logger.info("Attempting to close Solr Connection to: " + SOLR_URL);
        
        this.solr.close();
  
        logger.info("Closed Solr Connection to: " + SOLR_URL);
        return 0;
    }


    public String returnQuery(String myQuery) throws SolrServerException, IOException {
        this.start();

        SolrQuery query = new SolrQuery();
        QueryResponse response = null;
        query.set("q", myQuery);

        response = this.solr.query(query);

        this.close();
 

        if (response == null) return null;

        return response.getResults().toString();
    }

    public int deleteAllQuery() {
        try {
            this.solr.deleteByQuery("*");
            solr.commit();
        } catch (SolrServerException | IOException e) {
            logger.error(e.getMessage());
        }
        return 0;
    }
}
