🚘Real-Time Surge Pricing Pipeline (Kafka · PySpark · Elasticsearch)

📌 Project Overview
This project demonstrates a real-time data engineering pipeline that simulates ride-hailing surge pricing analytics using Apache Kafka, PySpark Structured Streaming, and Elasticsearch.

The pipeline:
1. Streams raw ride events into Kafka
2. Processes and aggregates data using PySpark
3. Publishes computed surge metrics back to Kafka
4. Indexes the results into Elasticsearch for search and analytics

This project showcases hands-on experience with stream processing, distributed systems, and geo-spatial data handling.

🏗 Architecture Overview
CSV File
  ↓
Kafka Producer (Source to Kafka.py)
  ↓
Kafka Topic: surgePriceDemo
  ↓
PySpark Structured Streaming (Spark Transformations.py)
  ↓
Kafka Topic: surge_pricing_demo_output
  ↓
Kafka Consumer (Consumer to ElasticSearch.py)
  ↓
Elasticsearch Index (rides_demo)

📂 Repository Structure
.
├── Source to Kafka.py
├── Spark Transformations.py
├── Consumer to ElasticSearch.py
└── README.md

🚨Future Enhancements
Add Kibana dashboards for surge heatmaps
Dockerize the entire pipeline
Deploy on Kubernetes

👤 Author
Shivani Jain
Data Engineer | PySpark | Kafka | Elasticsearch
