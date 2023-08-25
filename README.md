# Data Ingestor

Data Ingestor is a project designed to facilitate file uploads for data processing and querying using Apache Solr. The project provides an interface to upload, query, retrieve, and delete data.

## Table of Contents
- [Features](#features)
- [Usage](#usage)

## Features
1. **Upload Data** - Securely upload your data files for processing.
2. **Query Interface** - Utilize the power of Apache Solr to run complex queries.
3. **Delete Data** (Debug feature) - Clear all data. (This feature is only for debugging and not recommended for production)

## Usage

(Note: The following API endpoint usages are based on the API class provided.)

### Upload Data
To upload a file:
Use the following command:
curl http://localhost:8080/api/data -F "file={File path}"
curl http://localhost:8080/api/data -F "file=@{File path is local}"

Example:
curl http://localhost:8080/api/data -F "file=@product_feed_copy.csv"


### Query Interface
To query for specific data:
curl "http://localhost:8080/api/data?q={query}"

Example:
curl "http://localhost:8080/api/data?q=ProductID:30103472"

To retrieve all data:
curl "http://localhost:8080/api/data/"


### Delete All Data
*(Debug feature - Not recommended for production)*
curl -X DELETE http://localhost:8080/api/data/