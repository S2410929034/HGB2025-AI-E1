# Start the environment

Download the repository and start the environment:

```bash
docker compose up -d
```
## Verify the services
-Apache Pinot's Web UI: http://localhost:9000  

## Create a kafka topic:
```bash
docker exec \
  -t kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --partitions=1 --replication-factor=1 \
  --create --topic ingest-kafka
```

# Learn more about Apache Pinot
- Apache Pinot's home page: https://docs.pinot.apache.org/ 

# Basic setup

Understand the content of [ingest schema file](ingest_kafka_schema.json) and [table creation file](ingest_kafka_realtime_table.json). Then, navigate to Apache Pinot's Web UI and add a table schema and a realtime table. 

Navigate to ```Query Console``` and run your first query:

```
select * from ingest_kafka
```

More advanced query:

```
SELECT source_ip, COUNT(*) AS match_count FROM ingest_kafka
WHERE
  content LIKE '%vulnerability%' AND severity = 'High'
GROUP BY source_ip
ORDER BY match_count DESC    
```


See more about queries' syntax: https://docs.pinot.apache.org/users/user-guide-query

What are we missing when we execute the queries?

See how to ingest data on Apache Pinot: https://docs.pinot.apache.org/manage-data/data-import

# Load generator
Inside the ```load-generator``` folder, understand the content of the docker compose file and start generating log records: 
```bash
docker compose up -d
```

What is the relation between these records being generated and Apache Pinot's table?


Run again the advanced query:

```
SELECT source_ip, COUNT(*) AS match_count FROM ingest_kafka
WHERE
  content LIKE '%vulnerability%' AND severity = 'High'
GROUP BY source_ip
ORDER BY match_count DESC    
```

Are there any changes in query results? What and why?

Moreover, what is the amount of data in the table?

How this last query relates to the Spark Structured Streaming logs processing example from Exercise 3? 

How performant is the advanced query? How long it takes to run and how many queries like this one could be served per second?

Practical Exercise: From the material presented in the previous lecture on ``` Analytical Processing``` and Apache Pinot's features (available at https://docs.pinot.apache.org/ ), analyze and explain how the performance of the advanced query could be improved without demanding additional computing resources. Then, implement and demonstrate such an approach in Apache Pinot.

Foundational Exercise: Considering the material presented in the lecture ``` NoSQL - Data Processing & Advanced Topics``` and Apache Pinot's concepts https://docs.pinot.apache.org/basics/concepts and architecture https://docs.pinot.apache.org/basics/concepts/architecture, how an OLAP system such as Apache Pinot relates to NoSQL and realizes Sharding, Replication, and Distributed SQL?

## Expected Deliverables

Complete answers to all questions above, including brief analyses, configuration files, and performance metrics for the practical exercise.

## Clean up in the ```root folder``` and inside the ```load-generator``` folder. In both cases with the command:

```bash
docker compose down -v
```