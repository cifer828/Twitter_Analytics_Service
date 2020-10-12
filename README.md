## High-Performance Twitter Analytics Web Service
- Implemented ETL on `1TB` Twitter data using Spark and Hadoop and loaded data into MySQL and HBase as storage-tier.
- Constructed web-tier using SpringBoot and designed RESTful API for **user recommendation** and **topic extraction**.
- Orchestrated services on AWS EC2/Lightsail using Terraform and shell scripts.
- Utilized connection pools to improve storage-tier performance. 
- Monitored resources using jvmtop and profiled end-to-end latency to detect bottlenecks. 
- The service can handle more than `15K RPS` in the live tests.