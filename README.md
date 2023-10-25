# Distributed Spark Cluster to run ETL apps
This project represents a distributed etl application using Apache Spark. I built a local docker environment that deploys a network of the following services
- Postgres Database
- Spark Master
- 2 Spark Workers

The Spark master serves up a Web UI at http://localhost:8080/

# Getting Started

**Initialize and seed database**
```bash
make seed-db
```
**Build ETL services**
```bash
make build
```
**Spin up cluster**
```bash
make up
```
**Submit Spark app**
```bash
# Run daily calculation of Cases and Deaths app
make submit app_path=/app/daily_cases_deaths.py

# Run rolling avg app
make submit app_path=/app/rolling_avg.py

# Run top 10 net mask wear score app
make submit app_path=/app/top_nmw_score.py
```
**Spin down cluster**
```bash
make down
```

## Results
Find all report csv files in `reports/` directory. Once the apps run they will overwrite those reports. I mounted a docker volume so that reports get updated after each run.

Also, visit http://localhost:8080/ to explore app metrics.

## Possible Streaming Solution
If I were to process this data in a streaming app I would add Kafka and Kafka connect to to the picture. I stream change data capture events from the Postgres database to Kafka and then I would modify my apps to do structured streaming with Spark. Instead of writing to CSV, I would write to separate Kafka topic.

## Data

The data involved with this projects is copyrighted by The New York Times Company. The license can be found [here](https://github.com/nytimes/covid-19-data/blob/master/LICENSE).