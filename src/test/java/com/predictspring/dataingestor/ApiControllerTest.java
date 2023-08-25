package com.predictspring.dataingestor;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.predictspring.dataingestor.Api.ApiController;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.mock.web.MockMultipartFile;


@WebMvcTest(ApiController.class)
public class ApiControllerTest {

    @Autowired
    private MockMvc mockMvc;

    // @MockBean
    // private DatabaseController databaseController;

    // @MockBean
    // private SolrController solrController;

    @BeforeEach
    public void setup() {
        // This method can be used to set up any common data or mocks for each test
    }

    @Test
    public void testUploadFile() throws Exception {
        MockMultipartFile file 
            = new MockMultipartFile(
                "file", 
                "test.csv", 
                MediaType.TEXT_PLAIN_VALUE,
                "ProductID,Name,Description\nSTU901,Tennis Racket,Great for beginners.".getBytes()
            );

        mockMvc.perform(multipart("/api/data").file(file))
            .andExpect(status().isOk());
            // .andExpect(content().string("{\"status\":200,\"message\":File was properly uploaded}"));
    }

    @Test
    public void testGetQuery() throws Exception {
        mockMvc.perform(get("/api/data"))
        .andExpect(status().isOk());
        // mockMvc.perform(get("/api/data").param("q", "ProdudctID:nSTU901"))
        //     .andExpect(status().isOk());
    }
}
