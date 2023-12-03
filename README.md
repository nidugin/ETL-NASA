
# ELT NASA Pipeline 🔭 ✨

During my summer internship I have been developing a pipeline using [NASA Open API](https://api.nasa.gov/ "NASA Open API") as the source of the data. This pipeline collects at the same time data from the two sources: APOD and NeoWs. Data is sent to the Kafka topic and received at the Hadoop server. For the base layer it uses PySpark to apply for .json files schema and loads it to the Hive table. For the analytical layer it takes the data from the table to make transformations such as deleting duplicates, rows that don't suit mandatory fields and writing invalid data to the error table. The whole pipeline is automated by Airflow.
## Deployment

The project contains autotmaion with Airflow. If exists need to run layers manually then you can run these commands in the sequence.


```bash
  python3 producer_nikita.py {test/prod} {apod/prod} {api_key}
  spark-submit consumer_nikita.py {test/prod} {apod/prod}
  spark-submit base_nikita.py {test/prod} {apod/prod}
  spark-submit analytical_nikita.py {test/prod} {apod/prod}
```

## Config

In the config.ini you can change Kafka IP and topics name, paths of the future data directories for APOD and NeoWS sources.



## Support

For support and questions e-mail me: nikita@roldugins.com
